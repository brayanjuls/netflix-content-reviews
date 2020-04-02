from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

test_dag = DAG("test_enviroment")
