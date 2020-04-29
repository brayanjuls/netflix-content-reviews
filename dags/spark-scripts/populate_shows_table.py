from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
import pyspark.sql.types as tp
import sys


class ContentCommentsToBigQuery:

    def __init__(self, table_name, catalog_path, comments_path, temp_bucket):
        self.table_name = table_name
        self.catalog_path = catalog_path
        self.comments_path = comments_path
        self.temp_bucket = temp_bucket

    def get_df_schema(self):
        return tp.StructType([tp.StructField('added_date', tp.DateType(), True),
                              tp.StructField('release_year', tp.IntegerType(), True),
                              tp.StructField('title', tp.StringType(), False),
                              tp.StructField('type', tp.StringType(), False),
                              tp.StructField('duration', tp.StringType(), True),
                              tp.StructField('description', tp.StringType(), True),
                              tp.StructField('director', tp.StringType(), True),
                              tp.StructField('comments', tp.ArrayType(tp.StructType([
                                  tp.StructField('body', tp.StringType(), False),
                                  tp.StructField('author', tp.StringType(), False),
                                  tp.StructField('created_utc', tp.TimestampType(), False),
                                  tp.StructField('score', tp.IntegerType(), False),
                                  tp.StructField('sentiment', tp.StringType(), True),
                                  tp.StructField('description_word', tp.StringType(), True),
                                  tp.StructField('source', tp.StringType(), True)
                              ])), True),
                              tp.StructField('actors', tp.ArrayType(tp.StructType([
                                  tp.StructField('name', tp.StringType(), False)
                              ])), True)
                              ])

    def execute(self):
        spark = SparkSession.builder.appName('ContentReviewToBigQuery').getOrCreate()
        spark.conf.set('temporaryGcsBucket', self.temp_bucket)
        content_review_df = self.join_and_structure_dataset(spark)
        content_review_structured = spark.createDataFrame(content_review_df.rdd, self.get_df_schema())
        content_review_structured.write.format('bigquery').mode('Overwrite').option('table', self.table_name).save()

    def join_and_structure_dataset(self, spark):
        catalog_df = spark.read.parquet(self.catalog_path)
        comments_df = spark.read.parquet(self.comments_path)

        unnest_catalog = catalog_df.select(catalog_df.date_added, catalog_df.release_year, catalog_df.title,
                                           catalog_df.type, catalog_df.duration, catalog_df.description,
                                           catalog_df.director,
                                           fc.explode(fc.split(catalog_df.cast, ',')).alias('actor'))

        unnest_comments = comments_df.withColumn('comments', fc.explode(comments_df.comments).alias('comments')).select(
            comments_df.show_title, comments_df.source, comments_df.title,
            comments_df.description, comments_df.author,
            'comments.*')

        spark.catalog.dropTempView("catalog_temp")
        unnest_catalog.createTempView('catalog_temp')
        spark.catalog.dropTempView("comments_temp")
        unnest_comments.createTempView('comments_temp')

        structured_result = spark.sql(
            'select ct.date_added,ct.release_year,ct.title,ct.type,ct.duration,ct.description,ct.director, '
            'COLLECT_LIST(STRUCT(co.body as body,co.author,co.created_utc as created, co.score,null as sentiment, '
            'null as description_word, null as source)) as comments, '
            'COLLECT_LIST(STRUCT(ct.actor as name)) as actor '
            ' from catalog_temp ct inner join comments_temp co on ct.title=co.show_title '
            ' group by ct.date_added,ct.release_year,ct.title,ct.type,ct.duration,ct.description,ct.director')

        return structured_result


table_name_arg = sys.argv[1]
catalog_path_arg = sys.argv[2]
comments_path_arg = sys.argv[3]
temp_bucket_arg = sys.argv[4]

contentCommentsToBigQuery = ContentCommentsToBigQuery(table_name_arg, catalog_path_arg, comments_path_arg,
                                                      temp_bucket_arg)
contentCommentsToBigQuery.execute()
