from operators import (DownloadKaggleDataSet, NetflixCatalogToS3)
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow import DAG


def catalog_show_to_gcs(
        parent_dag_name,
        task_id,
        kaggle_bucket,
        kaggle_local_destination_path,
        gcp_conn_id,
        gcs_bucket,
        gcs_raw_destination_path,
        gcs_clean_destination_path,
        cluster_name,
        spark_code_path,
        region,
        *args, **kwargs
):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    download_kaggle_dataset = DownloadKaggleDataSet(task_id="DownloadKaggleDataSet", name=kaggle_bucket,
                                                    destination_path=kaggle_local_destination_path, dag=dag)

    copy_netflix_catalog = FileToGoogleCloudStorageOperator(task_id="NetflixCatalogToStagging",
                                                            src=kaggle_local_destination_path + "/netflix_titles.csv",

                                                            dst=gcs_raw_destination_path, bucket=gcs_bucket,
                                                            google_cloud_storage_conn_id=gcp_conn_id, dag=dag)
    destination_raw_path = 'gs://' + gcs_bucket + '/' + gcs_raw_destination_path
    destination_clean_path = 'gs://' + gcs_bucket + '/' + gcs_clean_destination_path
    clean_catalog_data = DataProcPySparkOperator(
        task_id='submit_clean_catalog_spark_job',
        main=spark_code_path,
        cluster_name=cluster_name,
        job_name='clean_catalog_data',
        region=region,
        arguments=[destination_raw_path, destination_clean_path],
        gcp_conn_id=gcp_conn_id
    )
    download_kaggle_dataset >> copy_netflix_catalog >> clean_catalog_data

    return dag
