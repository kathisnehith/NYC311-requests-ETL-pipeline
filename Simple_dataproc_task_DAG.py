import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# GCP Project and Dataproc Cluster details
PROJECT_ID = "serious-unison-441416-j6"
CLUSTER_NAME = "cluster-778a"
REGION = "us-central1"
PYSPARK_FETCH_JOB_FILE = "gs://nyc-311-requests/fetch_data_by_yr.py"
PYSPARK_TRANS_JOB_FILE = "gs://nyc-311-requests/nyc_datatransform_pysparkjob.py"

# Arguments to pass to the PySpark fetch job from Airflow Variables
FETCH_JOB_ARGS = [
    "--year", Variable.get("fetch_year", default_var="2025"),
    "--batch_size", Variable.get("fetch_batch_size", default_var="50000"),
    "--total_records", Variable.get("fetch_total_records", default_var="500000"),
]

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define the DAG
with models.DAG(
    "dynamic_spark_cluster",
    default_args=default_args,
    description='A dynamic DAG to fetch and transform data using Dataproc',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Fetch Task: Submitting PySpark fetch job to Dataproc
    PYSPARK_FETCH_JOB = {
        "placement": {
            "cluster_name": CLUSTER_NAME,
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_FETCH_JOB_FILE,
            "args": FETCH_JOB_ARGS,
        },
    }

    # Transformation Task: Submitting PySpark transformation job to Dataproc
    PYSPARK_TRANS_JOB = {
        "placement": {
            "cluster_name": CLUSTER_NAME,
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_TRANS_JOB_FILE,
        },
    }

    # Task to fetch data
    pyspark_fetch_task = DataprocSubmitJobOperator(
        job=PYSPARK_FETCH_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        task_id="pyspark_fetch_task",
    )

    # Task to transform data
    pyspark_transform_load_task = DataprocSubmitJobOperator(
        job=PYSPARK_TRANS_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        task_id="pyspark_transform_load_task",
    )

    # Set task dependencies
    pyspark_fetch_task >> pyspark_transform_load_task
