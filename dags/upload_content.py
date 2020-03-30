from airflow import DAG
from operators import (DownloadKaggleDataSet,
	NetflixCatalogToS3,RedditCommentToS3)
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
	'start_date': datetime(2019, 12, 1),
	'depends_on_past': False
}


dag = DAG("content_review",default_args=default_args)

start_pipeline = DummyOperator(task_id="StartPipeline",dag=dag)
download_kaggle_dataset = DownloadKaggleDataSet(task_id="DownloadKaggleDataSet",name="shivamb/netflix-shows",destination_path="/",dag=dag)
copy_netflix_catalog = NetflixCatalogToS3(task_id="NetflixCatalogToStagging",s3_bucket="",source_path="",dag=dag)
reddit_comment_to_staginng = RedditCommentToS3(task_id="RedditCommentToStagging",query_date="",client_id="",
	client_secret="",sub_reddits=["netflix","NetflixBestOf","bestofnetflix"],s3_bucket="",dag=dag)

end_pipeline = DummyOperator(task_id="EndPipeline",dag=dag)

start_pipeline >> download_kaggle_dataset >> copy_netflix_catalog
start_pipeline >> reddit_comment_to_staginng 
copy_netflix_catalog >> end_pipeline