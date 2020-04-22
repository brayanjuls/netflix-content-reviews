from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
from pyspark.sql.functions import column as col
import pyspark.sql.types as tp

# source_path = args[0]
# destination_path = args[1]

spark = SparkSession.builder.appName("NetflixCatalogToGCS").getOrCreate()
catalog_df = spark.read.csv('gs://netflix-content/catalog/raw/catalog.csv', inferSchema=True, header=True,
                            mode="DROPMALFORMED")
non_duplicated_content = catalog_df.dropDuplicates(['title', 'director']).orderBy(fc.desc('title'))
df_netflix_catalog = non_duplicated_content.dropna('any', subset=['title']).orderBy(fc.asc('title'))
df_netflix_catalog = df_netflix_catalog.withColumn('title', fc.translate('title', '"', ''))

df_netflix_catalog = df_netflix_catalog.withColumn('show_id', col('show_id').cast(tp.LongType()))
df_netflix_catalog = df_netflix_catalog.withColumn('release_year', col('release_year').cast(tp.IntegerType()))
df_netflix_catalog = df_netflix_catalog.withColumn('date_added', fc.to_date('date_added', 'MMMMM dd, yyyy'))
df_netflix_catalog.write.parquet('gs://netflix-content/catalog/clean/catalog.parquet', mode='overwrite')
print("Clean Catalog Executed")
