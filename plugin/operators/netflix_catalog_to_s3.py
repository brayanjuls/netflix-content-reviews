from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
from pyspark.sql.functions import column as col
import pyspark.sql.types as tp

class NetflixCatalogToS3(BaseOperator):

	@apply_defaults
	def __init__(self,
		s3_bucket,
		source_path,
		aws_credentials_id="",
		*args,  **kwargs):
		super(NetflixCatalogToS3, self).__init__(*args, **kwargs)
		self.s3_bucket=s3_bucket
		self.source_path=source_path
		self.aws_credentials_id=aws_credentials_id



	def execute(self, context):
		aws_hook = AwsHook(self.aws_credentials_id)
		credentials = aws_hook.get_credentials()

		spark = SparkSession.builder.appName("NetflixCatalogToS3").getOrCreate()
		spark._jsc.hadoopConfiguration().set("fs.s3a.access.key",credentials.access_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key",credentials.secret_key)
		spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint","s3.us-east-2.amazonaws.com")
		catalog_df = spark.read.csv(self.source_path,inferSchema=True,header=True,mode="DROPMALFORMED")

		non_duplicated_content=catalog_df.dropDuplicates(['title','director']).orderBy(fc.desc('title'))
		content_with_title=non_duplicated_content.dropna('any',subset=['title']).orderBy(fc.asc('title'))
		content_with_title=content_with_title.withColumn('title',fc.translate('title','"',''))

		content_with_title=content_with_title.withColumn('show_id',col('show_id').cast(tp.LongType()))
		content_with_title=content_with_title.withColumn('release_year',col('release_year').cast(tp.IntegerType()))
		content_with_title=content_with_title.withColumn('date_added',fc.to_date('date_added','MMMMM dd, yyyy'))
		full_bucket = "s3a://{}/".format(self.s3_bucket)
		content_with_title.write.parquet(full_bucket,mode='overwrite')

