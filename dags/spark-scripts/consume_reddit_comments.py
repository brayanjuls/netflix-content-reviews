import sys

from pyspark.sql import SparkSession
import pyspark.sql.types as tp
from datetime import datetime as dt
import praw


class RedditComments:

    def __init__(self, reddit_client_id, reddit_client_secret, user_agent, catalog_path, sub_reddits,
                 comments_destination_path):
        self.reddit_client_id = reddit_client_id
        self.reddit_client_secret = reddit_client_secret
        self.user_agent = user_agent
        self.catalog_path = catalog_path
        self.sub_reddits = sub_reddits
        self.comments_destination_path = comments_destination_path

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

    def getRedditComments(self, reddit, catalog_shows, sub_reddit, reddit_schema, spark, mylogger):
        content_rows = []
        mylogger.info("current execution subreddit {}".format(sub_reddit))
        for show in catalog_shows.collect():
            title_split = show.title.split(":", 1)
            content_title = title_split[0]
            subreddit = reddit.subreddit(sub_reddit)
            for sm in subreddit.search('"' + content_title + '"', sort='top'):
                sm.comments.replace_more(limit=None)
                row_comments = []
                for comment in sm.comments.list():
                    row_comments.append(
                        (comment.id, comment.body, dt.fromtimestamp(float(comment.created_utc)), comment.score,
                         comment.parent_id, comment.link_id))
                current_sm = (show.show_id, sm.id, subreddit.display_name, sm.title, sm.selftext,
                              dt.fromtimestamp(float(sm.created_utc)), sm.author.name,
                              sm.score, sm.spoiler, sm.is_original_content, sm.distinguished, sm.permalink,
                              row_comments)
                content_rows.append(current_sm)

        return spark.createDataFrame(content_rows, reddit_schema)

    def consumeRedditComments(self):
        spark = SparkSession.builder.appName("RedditCommentToGCP").getOrCreate()
        logger = spark._jvm.org.apache.log4j.Logger
        mylogger = logger.getLogger("RedditComments")

        reddit = self.getRedditInstance()
        reddit_schema = self.getRedditDataFrameSchema()

        df_netflix_shows_catalog = spark.read.parquet(self.catalog_path)
        reddits = self.sub_reddits.split()
        for sub_reddit in reddits:
            df_rdt_netflix_shows_comments = self.getRedditComments(reddit, df_netflix_shows_catalog, sub_reddit,
                                                                   reddit_schema,
                                                                   spark, mylogger)
            df_rdt_netflix_shows_comments.write.parquet(self.comments_destination_path, mode="ignore")


arg_reddit_client_id = sys.argv[1]
arg_reddit_client_secret = sys.argv[2]
arg_user_agent = "academic_comments_understanding:v1 by /u/zekeja"
arg_catalog_path = sys.argv[3]
arg_sub_reddits = sys.argv[4]
arg_comments_destination_path = sys.argv[5]
redditComments = RedditComments(arg_reddit_client_id, arg_reddit_client_secret, arg_user_agent, arg_catalog_path,
                                arg_sub_reddits,
                                arg_comments_destination_path)
redditComments.consumeRedditComments()
