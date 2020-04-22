from airflow import DAG
from operators import (RedditCommentToS3)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from catalog_show_to_gcs import catalog_show_to_gcs

start_date = datetime(2019, 12, 1)

default_args = {
    'start_date': start_date,
    'depends_on_past': False
}

dag = DAG("content_review", default_args=default_args, schedule_interval=None)

start_pipeline = DummyOperator(task_id="StartPipeline", dag=dag)

cluster_name = 'etl-content-{{ ds }}'
gcs_netflix_bucket = "netflix-content"
gcp_conn = "google_cloud_connection"
create_dataproc_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name=cluster_name,
    project_id=Variable.get('gc_project_id'),
    gcp_conn_id=gcp_conn,
    num_workers=2,
    num_masters=1,
    image_version='preview',
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    worker_disk_size=50,
    master_disk_size=50,
    region=Variable.get('gc_region'),
    storage_bucket="netflix-content",
    dag=dag
)

upload_spark_job_to_gcs = FileToGoogleCloudStorageOperator(task_id="spark_jobs_to_gcs",
                                                           src="/airflow/dags/spark-scripts/clean_netflix_catalog.py",
                                                           dst="spark-jobs/clean_netflix_catalog.py",
                                                           bucket=gcs_netflix_bucket,
                                                           google_cloud_storage_conn_id=gcp_conn,
                                                           dag=dag)

catalog_task_id = "show_catalog_subdag"
download_catalog_show_subdag = SubDagOperator(
    subdag=catalog_show_to_gcs(
        "content_review",
        catalog_task_id,
        kaggle_bucket="shivamb/netflix-shows",
        kaggle_local_destination_path="/airflow/datasources/catalog/csv",
        gcp_conn_id=gcp_conn,
        gcs_bucket=gcs_netflix_bucket,
        gcs_raw_destination_path="catalog/raw/catalog.csv",
        gcs_clean_destination_path="catalog/clean/catalog.parquet",
        cluster_name=cluster_name,
        spark_code_path="gs://" + gcs_netflix_bucket + "/spark-jobs/clean_netflix_catalog.py",
        region=Variable.get('gc_region'),
        start_date=start_date
    ),
    task_id=catalog_task_id,
    dag=dag
)

# reddit_comment_to_staginng = RedditCommentToS3(task_id="RedditCommentToStagging",query_date="{ds}",
#	reddit_client_id=Variable.get("reddit_client_id"), reddit_client_secret=Variable.get("reddit_client_secret"),
#	sub_reddits=["netflix","NetflixBestOf","bestofnetflix"], s3_load_bucket="/airflow/datasources/comments/parquet", s3_catalog_bucket="/airflow/datasources/catalog/parquet", 
#	aws_credentials_id="aws_credentials", dag=dag)

delete_dataproc_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name=cluster_name,
    project_id=Variable.get('gc_project_id'),
    gcp_conn_id=gcp_conn,
    region=Variable.get('gc_region'),
    dag=dag
)

end_pipeline = DummyOperator(task_id="EndPipeline", dag=dag)

start_pipeline >> [create_dataproc_cluster, upload_spark_job_to_gcs]
[create_dataproc_cluster, upload_spark_job_to_gcs] >> download_catalog_show_subdag
download_catalog_show_subdag >> delete_dataproc_cluster
delete_dataproc_cluster >> end_pipeline
