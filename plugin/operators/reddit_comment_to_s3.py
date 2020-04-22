from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from pyspark.sql import SparkSession
import pyspark.sql.types as tp
from datetime import datetime as dt
import praw


class RedditCommentToS3(BaseOperator):

    @apply_defaults
    def __init__(self,
                 query_date,
                 reddit_client_id,
                 reddit_client_secret,
                 sub_reddits,
                 s3_load_bucket,
                 s3_catalog_bucket,
                 aws_credentials_id,
                 user_agent="academic_comments_understanding:v1 by /u/zekeja",
                 *args, **kwargs):
        super(RedditCommentToS3, self).__init__(*args, **kwargs)
        self.query_date = query_date
        self.reddit_client_id = reddit_client_id
        self.reddit_client_secret = reddit_client_secret
        self.sub_reddits = sub_reddits
        self.s3_catalog_bucket = s3_catalog_bucket
        self.s3_load_bucket = s3_load_bucket
        self.user_agent = user_agent
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3").appName(
            "RedditCommentToS3").getOrCreate()
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", credentials.access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", credentials.secret_key)
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                             "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

        full_catalog_bucket = "s3a://{}/".format(self.s3_catalog_bucket)
        df_netflix_shows_catalog = spark.read.parquet(self.s3_catalog_bucket)

        full_load_bucket = "s3a://{}/".format(self.s3_load_bucket)
        reddit = self.getRedditInstance()
        reddit_schema = self.getRedditDataFrameSchema()

        for sub_reddit in self.sub_reddits:
            df_rdt_netflix_shows_comments = self.getRedditComments(reddit, df_netflix_shows_catalog, sub_reddit,
                                                                   reddit_schema, spark)
            df_rdt_netflix_shows_comments.write.parquet(self.s3_load_bucket, mode="ignore")

        self.log.info("reddit comment finished")

    def getRedditInstance(self):
        return praw.Reddit(client_id=self.reddit_client_id, client_secret=self.reddit_client_secret
                           ,
                           user_agent=self.user_agent)

    def getRedditDataFrameSchema(self):
        return tp.StructType([tp.StructField('show_id', tp.LongType(), True),
                              tp.StructField('submission_id', tp.StringType(), True),
                              tp.StructField('source', tp.StringType(), True),
                              tp.StructField('title', tp.StringType(), True),
                              tp.StructField('description', tp.StringType(), True),
                              tp.StructField('created_utc', tp.TimestampType(), True),
                              tp.StructField('author', tp.StringType(), True),
                              tp.StructField('score', tp.IntegerType(), True),
                              tp.StructField('spoiler', tp.BooleanType(), True),
                              tp.StructField('is_original_content', tp.BooleanType(), True),
                              tp.StructField('distinguished', tp.StringType(), True),
                              tp.StructField('link', tp.StringType(), True),
                              tp.StructField('comments', tp.ArrayType(tp.StructType([
                                  tp.StructField('comment_id', tp.StringType(), True),
                                  tp.StructField('body', tp.StringType(), True),
                                  tp.StructField('created_utc', tp.TimestampType(), True),
                                  tp.StructField('score', tp.IntegerType(), True),
                                  tp.StructField('parent_id', tp.StringType(), True),
                                  tp.StructField('submission_id', tp.StringType(), True)]
                              )), True)
                              ])

    def getRedditComments(self, reddit, netflix_shows_catalog, sub_reddit, reddit_schema, spark):
        content_rows = []
        for content in netflix_shows_catalog.limit(100).collect():
            title_split = content.title.split(":", 1)
            content_title = title_split[0]
            subreddit = reddit.subreddit(sub_reddit)
            for sm in subreddit.search('"' + content_title + '"', sort='new'):
                sm.comments.replace_more(limit=None)
                row_comments = []
                for comment in sm.comments.list():
                    row_comments.append(
                        (comment.id, comment.body, dt.fromtimestamp(float(comment.created_utc)), comment.score,
                         comment.parent_id, comment.link_id))
                current_sm = (content.show_id, sm.id, subreddit.display_name, sm.title, sm.selftext,
                              dt.fromtimestamp(float(sm.created_utc)), sm.author.name,
                              sm.score, sm.spoiler, sm.is_original_content, sm.distinguished, sm.permalink,
                              row_comments)
                content_rows.append(current_sm)

        self.log.info('Data extracted from subreddit: {}'.format(sub_reddit))
        return spark.createDataFrame(content_rows, reddit_schema)
