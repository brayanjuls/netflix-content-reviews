from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class NetflixCatalogToS3(BaseOperator):

	@apply_defaults
	def __init__(self,
		s3_bucket,
		source_path,
		*args,  **kwargs):
		super(NetflixCatalogToS3, self).__init__(*args, **kwargs)
		self.s3_bucket=s3_bucket
		self.source_path=source_path



	def execute(self, context):
		pass