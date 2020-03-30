from airflow.plugins_manager import AirflowPlugin

import operators

class ContentNetflixPlugin(AirflowPlugin):
    name = "content_netflix"
    operators = [
    	operators.NetflixCatalogToS3,
    	operators.RedditCommentToS3,
    	operators.DownloadKaggleDataSet
    ]