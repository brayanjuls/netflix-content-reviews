from airflow import DAG
from operators import (DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataprocClusterDeleteOperator,DataProcPySparkOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator,BigQueryCreateEmptyDatasetOperator
from airflow.models import Variable
from datetime import datetime,timedelta
from catalog_show_to_gcs import catalog_show_to_gcs

start_date = datetime(2019, 12, 1)

default_args = {
	'start_date': start_date,
	'depends_on_past': False
}


dag = DAG("content_review",default_args=default_args,schedule_interval=None)

start_pipeline = DummyOperator(task_id="StartPipeline",dag=dag)

cluster_name='etl-content-{{ ds }}'
gcs_netflix_bucket = "netflix-content"
gcp_conn="google_cloud_connection"
region = Variable.get('gc_region')
create_dataproc_cluster = DataprocClusterCreateOperator(
       task_id='create_dataproc_cluster',
       cluster_name=cluster_name,
       project_id= Variable.get('gc_project_id'),
       gcp_conn_id=gcp_conn,
       init_actions_uris=['gs://dataproc-initialization-actions/python/pip-install.sh'],
       metadata={'PIP_PACKAGES': 'praw==6.5.1'},
       num_workers=2,
       num_masters=1,
       image_version='preview',
       master_machine_type='n1-standard-2',
       worker_machine_type='n1-standard-2',
       worker_disk_size=50,
       master_disk_size=50,
       region=region,
       storage_bucket = gcs_netflix_bucket,
       dag=dag
   )

upload_netflix_catalog_job_to_gcs = FileToGoogleCloudStorageOperator(task_id="upload_netflix_catalog_job_to_gcs", 
	src="/airflow/dags/spark-scripts/clean_netflix_catalog.py",
	dst="spark-jobs/clean_netflix_catalog.py", 
	bucket=gcs_netflix_bucket, 
	google_cloud_storage_conn_id=gcp_conn, 
	dag=dag)

upload_reddit_comments_job_to_gcs = FileToGoogleCloudStorageOperator(task_id="upload_reddit_comments_job_to_gcs", 
	src="/airflow/dags/spark-scripts/consume_reddit_comments.py",
	dst="spark-jobs/consume_reddit_comments.py", 
	bucket=gcs_netflix_bucket, 
	google_cloud_storage_conn_id=gcp_conn, 
	dag=dag)

upload_populate_shows_table_job_to_gcs = FileToGoogleCloudStorageOperator(task_id="upload_populate_shows_table_job_to_gcs", 
	src="/airflow/dags/spark-scripts/populate_shows_table.py",
	dst="spark-jobs/populate_shows_table.py", 
	bucket=gcs_netflix_bucket, 
	google_cloud_storage_conn_id=gcp_conn, 
	dag=dag)

upload_generate_show_comments_job_to_gcs = FileToGoogleCloudStorageOperator(task_id="upload_generate_show_comments_job_to_gcs", 
	src="/airflow/dags/spark-scripts/generate_show_comments.py",
	dst="spark-jobs/generate_show_comments.py", 
	bucket=gcs_netflix_bucket, 
	google_cloud_storage_conn_id=gcp_conn, 
	dag=dag)

catalog_task_id = "show_catalog_subdag"
catalog_path="catalog/clean/catalog.parquet"
# download_catalog_show_subdag = SubDagOperator(
#     subdag=catalog_show_to_gcs(
#         "content_review",
#         catalog_task_id,
#         kaggle_bucket="shivamb/netflix-shows",
#         kaggle_local_destination_path="/airflow/datasources/catalog/csv",
#         gcp_conn_id=gcp_conn,
#         gcs_bucket=gcs_netflix_bucket,
#         gcs_raw_destination_path="catalog/raw/catalog.csv",
#         gcs_clean_destination_path=catalog_path,
#         cluster_name=cluster_name,
#         spark_code_path="gs://"+gcs_netflix_bucket+"/spark-jobs/clean_netflix_catalog.py",
#         region=region,
#         start_date=start_date
#     ),
#     task_id=catalog_task_id,
#     dag=dag
# 	)

consume_show_comments_job_path="gs://"+gcs_netflix_bucket+"/spark-jobs/consume_reddit_comments.py"
reddit_destination_path="gs://"+gcs_netflix_bucket+"/comments/raw/comments.parquet"
gcp_netflix_catalog_path="gs://"+gcs_netflix_bucket+"/"+catalog_path

consume_show_comment_to_datalake = DataProcPySparkOperator(
    task_id = 'consume_show_comment_to_datalake',
    main= consume_show_comments_job_path,
    cluster_name=cluster_name,
    job_name= 'consume_show_comments',
    region=region,
    arguments=[Variable.get("reddit_client_id"),Variable.get("reddit_client_secret"),
    gcp_netflix_catalog_path,["netflix NetflixBestOf bestofnetflix"],reddit_destination_path],
    gcp_conn_id=gcp_conn,
    dag=dag
    )

generate_show_comments_job_path = "gs://"+gcs_netflix_bucket+"/spark-jobs/generate_show_comments.py"
generate_show_comment_to_datalake = DataProcPySparkOperator(
    task_id = 'generate_show_comment_to_datalake',
    main= generate_show_comments_job_path,
    cluster_name=cluster_name,
    job_name= 'generate_show_comments',
    region=region,
    arguments=[gcp_netflix_catalog_path,reddit_destination_path],
    gcp_conn_id=gcp_conn,
    dag=dag
    )

def is_consume_show_comments_enable():
	consume_comments = Variable.get('is_consume_show_comments_enable')
	if consume_comments == "True":
		return consume_show_comment_to_datalake.task_id
	else:
		return generate_show_comment_to_datalake.task_id

consume_or_generate_comments = BranchPythonOperator(
	task_id='consume_or_generate_comments',
	 python_callable=is_consume_show_comments_enable,
	 dag=dag)



dataset_id="shows_comments"
create_shows_comments_dataset= BigQueryCreateEmptyDatasetOperator(
	task_id='create_shows_comments_dataset',
	project_id=Variable.get('gc_project_id'),
	dataset_id=dataset_id,
	bigquery_conn_id=gcp_conn,
	trigger_rule="one_success",
	dag=dag
	)

shows_table_name="shows"
# create_shows_table= BigQueryCreateEmptyTableOperator(
# 	task_id="create_shows_table",
# 	project_id=Variable.get('gc_project_id'),
# 	dataset_id=dataset_id,
# 	bigquery_conn_id=gcp_conn,
# 	table_id=shows_table_name,
# 	schema_fields=[{"name":"release_year","type":"INTEGER","mode":"NULLABLE"},
# 	{"name":"added_date","type":"DATE","mode":"NULLABLE"},
# 	{"name":"title","type":"STRING","mode":"REQUIRED"},
# 	{"name":"type","type":"STRING","mode":"REQUIRED"},
# 	{"name":"duration","type":"STRING","mode":"NULLABLE"},
# 	{"name":"description","type":"STRING","mode":"NULLABLE"},
# 	{"name":"director","type":"STRING","mode":"NULLABLE"},
# 	{"name":"comments","type":"RECORD","mode":"REPEATED",
# 	"fields":[
# 		{"name":"body","type":"STRING","mode":"NULLABLE"},
# 		{"name":"author","type":"STRING","mode":"NULLABLE"},
# 		{"name":"created_utc","type":"TIMESTAMP","mode":"NULLABLE"},
# 		{"name":"score","type":"INTEGER","mode":"NULLABLE"},
# 		{"name":"sentiment","type":"STRING","mode":"NULLABLE"},
# 		{"name":"description_word","type":"STRING","mode":"NULLABLE"}
# 	]},
# 	{"name":"actors","type":"RECORD","mode":"REPEATED",
# 	"fields":[
# 		{"name":"name","type":"STRING","mode":"NULLABLE"}	
# 	]},
# 	],
# 	dag=dag
# 	)

populate_shows_table_job_path="gs://"+gcs_netflix_bucket+"/spark-jobs/populate_shows_table.py"
populate_shows_table = DataProcPySparkOperator(
	task_id='populate_shows_table',
	main=populate_shows_table_job_path,
	cluster_name=cluster_name,
    job_name= 'content_comments_to_bigquery',
    region=region,
    arguments=["{}.{}".format(dataset_id,shows_table_name),gcp_netflix_catalog_path,reddit_destination_path,gcs_netflix_bucket],
    dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',
    gcp_conn_id=gcp_conn,
    dag=dag
	)

delete_dataproc_cluster = DataprocClusterDeleteOperator(
   task_id='delete_dataproc_cluster',
   cluster_name=cluster_name,
   project_id=Variable.get('gc_project_id'),
   gcp_conn_id=gcp_conn,
   region=Variable.get('gc_region'),
   dag=dag
   )

end_pipeline = DummyOperator(task_id="EndPipeline",dag=dag)

#start_pipeline >>  create_dataproc_cluster >> 
start_pipeline >> create_dataproc_cluster >> [upload_netflix_catalog_job_to_gcs,upload_reddit_comments_job_to_gcs,upload_populate_shows_table_job_to_gcs,upload_generate_show_comments_job_to_gcs]
#[upload_netflix_catalog_job_to_gcs,upload_reddit_comments_job_to_gcs,upload_populate_shows_table_job_to_gcs,upload_generate_show_comments_job_to_gcs] >> download_catalog_show_subdag
[upload_netflix_catalog_job_to_gcs,upload_reddit_comments_job_to_gcs,upload_populate_shows_table_job_to_gcs,upload_generate_show_comments_job_to_gcs] >> consume_or_generate_comments >> [consume_show_comment_to_datalake,generate_show_comment_to_datalake]
[consume_show_comment_to_datalake,generate_show_comment_to_datalake] >> create_shows_comments_dataset
create_shows_comments_dataset >> populate_shows_table >> delete_dataproc_cluster >> end_pipeline