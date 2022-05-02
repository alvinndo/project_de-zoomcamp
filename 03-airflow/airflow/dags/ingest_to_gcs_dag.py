import os
import logging

from datetime import datetime

import pandas as pd

from itertools import chain
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import split, create_map, lit

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

dataset_year = '{{ execution_date.strftime("%y") }}'
dataset_prefix = "https://www.bls.gov/lau/"
dataset_file = f'laucnty{dataset_year}.xlsx'
dataset_url = dataset_prefix + dataset_file

dataset_pq = dataset_file.replace('.xlsx', '.parquet')

def format_xl_to_pq(src_file, year):
    if not src_file.endswith('.xlsx'):
        logging.error("Can only accept source files in XLSX format, for the moment")
        return

    # reading excel
    df = pd.read_excel(src_file)

    # dropping and replacing values
    df.drop(index=df.index[:5], axis=0, inplace=True)
    df.drop(index=df.tail(3).index, axis=0, inplace=True)
    df.drop(columns="Unnamed: 5", inplace=True)
    df.replace(to_replace="N.A.", value=0, inplace=True)

    # renaming columns
    df.columns = ["LAUS_code", "state_fips", "county_fips", "county_state", "year", "labor_force", "employed", "unemployed", "unemployment_rate"]

    # combining fips
    df['fips'] = df['state_fips'] + df['county_fips']

    # updating year and rate
    df.year = pd.to_numeric(df.year)
    df.unemployment_rate = pd.to_numeric(df.unemployment_rate)

    # writing to table
    df.to_parquet(f"{AIRFLOW_HOME}/us_employment_{year}.parquet")
    

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

with DAG(
    dag_id="data_ingestion_gcp_dag",
    schedule_interval="@yearly",
    start_date = datetime(1990, 1, 1),
    end_date = datetime(2021, 12, 1),
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
        },
    catchup=True,
    max_active_runs=3,
    tags=['de-project']
) as dag:

    EXEC_YEAR = '{{ execution_date.strftime("%Y") }}'

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {AIRFLOW_HOME}/{dataset_file}"
    )

    format_xl_to_pq_task = PythonOperator(
        task_id="format_xl_to_pq_task",
        python_callable=format_xl_to_pq,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{dataset_file}",
            "year": EXEC_YEAR
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/us_employment_{EXEC_YEAR}.parquet",
            "local_file": f"{AIRFLOW_HOME}/us_employment_{EXEC_YEAR}.parquet",
        }
    )

    download_dataset_task >> format_xl_to_pq_task >> local_to_gcs_task 
