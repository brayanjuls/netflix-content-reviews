import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
from pyspark.sql.functions import column as col
import pyspark.sql.types as tp


class NetflixCatalog:

    def __init__(self,
                 source_path,
                 destination_path):
        self.source_path = source_path
        self.destination_path = destination_path
        self.spark = SparkSession.builder.appName("NetflixCatalogToGCS").getOrCreate()

    def clean(self):
        catalog_df = self.spark.read.csv(self.source_path, inferSchema=True, header=True,
                                         mode="DROPMALFORMED")
        non_duplicated_content = catalog_df.dropDuplicates(['title', 'director']).orderBy(fc.desc('title'))
        df_netflix_catalog = non_duplicated_content.dropna('any', subset=['title','director']).orderBy(fc.asc('title'))
        df_netflix_catalog = df_netflix_catalog.withColumn('title', fc.translate('title', '"', ''))

        df_netflix_catalog = df_netflix_catalog.withColumn('show_id', col('show_id').cast(tp.LongType()))
        df_netflix_catalog = df_netflix_catalog.withColumn('release_year', col('release_year').cast(tp.IntegerType()))
        df_netflix_catalog = df_netflix_catalog.withColumn('date_added', fc.to_date('date_added', 'MMMMM dd, yyyy'))
        df_netflix_catalog.write.partitionBy(['title', 'director']).bucketBy(2,"release_year").parquet(self.destination_path,
                                         mode='overwrite')
        print("Clean Catalog Executed")


arg_source_path = sys.argv[1]
arg_destination_path = sys.argv[2]

netflixCatalog = NetflixCatalog(arg_source_path, arg_destination_path)
netflixCatalog.clean()
