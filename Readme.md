# Netflix Content Review

## Summary
This project is a data pipeline created with the intention of generating data related to netflix's content opinion on reddit, this data will serve a twitter bot that will tweet every time someone write on reddit about a certain movie o serie that is on netflix content catalog, also a datawarehouse will be created to serve an analytics dashboard where we will answers a few questions. To achieve this we used the following datasources:

* [Kaggle Netflix Movies and TV Shows DataSet](https://www.kaggle.com/shivamb/netflix-shows/data#) 
* [Reddit api](https://www.reddit.com/dev/api)

## Architecture 

![architecture](resources/Netflix_content_review_GCP.jpg)

#### BigQuery table structure
![big_query_table_structure](resources/shows_bigquery_table.png)

## Pre Requisite
Python 3.5 or later
Jupyter Notebook 

## Configuration
* The following docker image was used to deploy the project [airflow-pipeline](https://github.com/brayanjuls/airflow-pipeline)

* praw.ini = File to add properties related to reddit like the authentication credentials

* XDG_CONFIG_HOME = Enviroment variable to set the folder path where the praw.init file is going to live
