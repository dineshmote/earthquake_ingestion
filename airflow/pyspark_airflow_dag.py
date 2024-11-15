from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date':days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# # Cluster configuration
cluster_config = {
    'project_id':'gcp-data-project-440907',
    'cluster_name': 'dataproc-pyspark-cluster2',
    'region': 'us-east4',
    'config': {
        'master_config': {
            'num_instances': 1,
            'machine_type_uri': 'e2-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50}  # Adjust disk size here
        },
        'worker_config': {
            'num_instances': 2,
            'machine_type_uri': 'e2-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50}  # Adjust disk size here
        },
        'software_config': {
            'image_version': '2.1'
        }
    }
}

# Spark job configuration
DATAPROC_JOB1 = {
    "reference": {"project_id": 'gcp-data-project-440907'},
    "placement": {"cluster_name": cluster_config['cluster_name']},
    "pyspark_job": {
        "main_python_file_uri": "gs://airflow_dag_file/pyspark/bronze/load_daily_data.py",
        "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar" ],
        "python_file_uris": ["gs://airflow_dag_file/pyspark/bronze/util.py" ]
    }
}
DATAPROC_JOB2 = {
    "reference": {"project_id": 'gcp-data-project-440907'},
    "placement": {"cluster_name": cluster_config['cluster_name']},
    "pyspark_job": {
        "main_python_file_uri": "gs://airflow_dag_file/pyspark/silver/daily_data_transformation.py",
        "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar" ],
        "python_file_uris": ["gs://airflow_dag_file/pyspark/silver/util.py" ]
    }
}

# Define the DAG
with DAG(
    "earthquake_daily_data_pyspark",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task to create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=cluster_config['project_id'],
        cluster_config=cluster_config['config'],
        region=cluster_config['region'],
        cluster_name=cluster_config['cluster_name'],
        gcp_conn_id='google_cloud_default'
    )

    # Task to submit Spark job
    submit_dataproc_job1 = DataprocSubmitJobOperator(
        task_id='fetch_data_job',
        job=DATAPROC_JOB1,
        region=cluster_config['region'],
        project_id=cluster_config['project_id'],
        gcp_conn_id='google_cloud_default'
    )
    
    submit_dataproc_job2 = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        job=DATAPROC_JOB2,
        region=cluster_config['region'],
        project_id=cluster_config['project_id'],
        gcp_conn_id='google_cloud_default'
    )
    # Task to delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=cluster_config['cluster_name'],
        project_id=cluster_config['project_id'],
        region=cluster_config['region'],
        trigger_rule='all_done',  # Ensures cluster is deleted even if the job fails
        gcp_conn_id='google_cloud_default'
    )

    # Set task dependencies
    create_cluster >> submit_dataproc_job1 >> submit_dataproc_job2 >> delete_cluster