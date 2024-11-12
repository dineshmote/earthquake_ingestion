from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG(
    dag_id="daily_data_proc_job",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Define the first PySpark job to fetch API data and write to GCS
    initial_pyspark_job = {
        "reference": {"project_id": "gcp-data-project-440907"},
        "placement": {"cluster_name": "cluster-ebd7"},
        "pyspark_job": {
            "main_python_file_uri": "gs://your-bucket-path/initial_data_fetch.py",  # Path to the initial main file in GCS
            "python_file_uris": ["gs://your-bucket-path/util.py"],  # Path to the util.py in GCS
            "args": [
                "--gcs_bucket_name", "earthquake_analysis_data1",
                "--api_url", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
                "--project_id", "gcp-data-project-440907"
            ]
        }
    }

    # Operator for the first PySpark job
    fetch_data_job = DataprocSubmitJobOperator(
        task_id='fetch_data_job',
        job=initial_pyspark_job,
        region="us-east4",
        project_id="gcp-data-project-440907"
    )

    # Define the second PySpark job to process the data
    pyspark_job = {
        "reference": {"project_id": "gcp-data-project-440907"},
        "placement": {"cluster_name": "cluster-ebd7"},
        "pyspark_job": {
            "main_python_file_uri": "/home/dineshmote861/earthquake_ingestion/silver/daily_data_transformation.py",
            "python_file_uris": ["/home/dineshmote861/earthquake_ingestion/silver/util.py"],
            "args": [
                "--gcs_bucket_name", "earthquake_analysis_data1",
                "--project_id", "gcp-data-project-440907",
                "--dataset_id", "earthquake_ingestion",
                "--table_id", "earthquake_data",
                "--gcs_temp_location", "earthquake_analysis_data_bucket"
            ]
        }
    }

    # Operator for the second PySpark job
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=pyspark_job,
        region="us-east4",
        project_id="gcp-data-project-440907"
    )

    # Set task dependencies to run the second job after the first completes
    fetch_data_job >> submit_pyspark_job
