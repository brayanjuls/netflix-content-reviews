from airflow.plugins_manager import AirflowPlugin

import operators

class ContentNetflixPlugin(AirflowPlugin):
    name = "content_netflix"
    operators = [
    	operators.DataQualityOperator,
    	operators.DownloadKaggleDataSet
    ]