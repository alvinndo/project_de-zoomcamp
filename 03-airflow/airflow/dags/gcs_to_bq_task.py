from ingest_to_gcs_dag import upload_to_gcs
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import split, create_map, lit

from itertools import chain

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
SPARK_JAR = os.environ.get("SPARK_JAR_DIR")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'us_employment_all')

def process_data():
    states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
        }

    # conf = SparkConf().setMaster('local[*]').setAppName('de-project-spark-batch').set("spark.jars", jar_file).set("spark.hadoop.google.cloud.auth.service.account.enable", "true").set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials)

    # sc = SparkContext(conf=conf)

    # hadoop_conf = sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    # hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    # hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials)
    # hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    # spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

    spark = SparkSession.builder.master("local[*]").appName('de-project').getOrCreate()

    df_emp = spark.read.option("header", "true").parquet(f'{AIRFLOW_HOME}/us_employment_*.parquet')

    df_emp = df_emp.withColumn('state', split(df_emp['county_state'], ', ').getItem(1)).withColumn('county', split(df_emp['county_state'], ', ').getItem(0))

    mapping_expr = create_map([lit(x) for x in chain(*states.items())])

    df_emp = df_emp.withColumn('state_full', mapping_expr[df_emp.state])

    df_emp = df_emp.select("year", "fips", "state_fips", "county_fips", "state", "state_full", "county", "labor_force", "employed", "unemployed", "unemployment_rate")

    df_emp.write.parquet(f'{AIRFLOW_HOME}/data/', mode='overwrite')

    spark.stop()

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "gcs_to_bq_dag",
    schedule_interval="@yearly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    process_data_from_gcs_task = PythonOperator(
        task_id="process_data_from_gcs_task",
        python_callable=process_data
    )

    rename_datasets_task = BashOperator(
        task_id="rename_datasets_task",
        bash_command=f"cat {AIRFLOW_HOME}/data/*.snappy.parquet > {AIRFLOW_HOME}/data/us_employment.snappy.parquet"
    )

    processed_to_gcs_task = PythonOperator(
        task_id="processed_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"processed/us_employment.snappy.parquet",
            "local_file": f"{AIRFLOW_HOME}/data/us_employment.snappy.parquet",
        }
    )

    bq_external_table_task = BigQueryCreateExternalTableOperator(
        task_id = "bq_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "us_employment_external_table_prod"
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/processed/part-*"]
            }
        }
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.us_employment_partitioned \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.us_employment_external_table;"
    )

    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    rm_files_task = BashOperator(
        task_id="rm_files_task",
        bash_command=f"rm {AIRFLOW_HOME}/*.xlsx {AIRFLOW_HOME}/us_employment_*.parquet {AIRFLOW_HOME}/data/*.snappy.parquet"
    )

    process_data_from_gcs_task >> rename_datasets_task >> processed_to_gcs_task >> bq_external_table_task >> bq_create_partitioned_table_job >> rm_files_task