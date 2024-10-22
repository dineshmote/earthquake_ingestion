import json
import requests
from datetime import datetime
from google.cloud import storage, bigquery   
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType, LongType
from pyspark.sql.functions import col, split, current_timestamp
import os

def fetch_data_from_api(api_url):
    """
    Fetch data from a given API endpoint.
    
    :param api_url: URL of the API to fetch data from.
    :return: Parsed JSON data from the API response.
    """
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def write_data_to_gcs(data, bucket_name, file_name):
    """
    Write the API data to GCS as a JSON file using GCS Client Libraries.
    
    :param data: Data fetched from the API (Python dictionary).
    :param bucket_name: Name of the GCS bucket.
    :param file_name: File name to save the data as in the GCS bucket.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    json_data = json.dumps(data)
    blob.upload_from_string(json_data, content_type='application/json')
    print(f"Data written to GCS bucket {bucket_name} as {file_name}.")

def read_data_from_gcs(bucket_name, file_name):
    """
    Read JSON data from GCS and return it as a Python dictionary.
    
    :param bucket_name: GCS bucket name.
    :param file_name: Name of the file to read from the GCS bucket.
    :return: Parsed JSON data from GCS file.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    data = blob.download_as_string()
    return json.loads(data)

def convert_timestamp_to_gmt(timestamp_ms):
    """
    Convert Unix timestamp in milliseconds to GMT.
    
    :param timestamp_ms: Unix timestamp in milliseconds.
    :return: Formatted GMT string.
    """
    if timestamp_ms is not None:
        # Convert milliseconds to seconds
        timestamp_s = timestamp_ms / 1000
        # Convert to datetime in UTC and format to string
        return datetime.utcfromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
    return None

def initialize_spark(app_name): 
    """
    Initialize PySpark session with Google Cloud Storage connector.
    
    :param app_name: Name of the Spark application.
    :return: SparkSession object.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def transform_data_to_df(spark, json_data):
    """
    Transform JSON data into a PySpark DataFrame with a predefined schema.
    
    :param spark: SparkSession object.
    :param json_data: Parsed JSON data to be transformed.
    :return: DataFrame with transformed data.
    """
    features = json_data.get("features", [])
    flatten_data = []

    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]

        flattened_record = {
            # "id": properties.get("id"),
            "place": properties.get("place"),
            "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
            "time": convert_timestamp_to_gmt(properties.get("time")),
            "updated": convert_timestamp_to_gmt(properties.get("updated")),
            "tz": properties.get("tz"),
            "url": properties.get("url"),
            "detail": properties.get("detail"),
            "felt": properties.get("felt"),
            "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
            "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
            "alert": properties.get("alert"),
            "status": properties.get("status"),
            "tsunami": properties.get("tsunami"),
            "sig": properties.get("sig"),
            "net": properties.get("net"),
            "code": properties.get("code"),
            "ids": properties.get("ids"),
            "sources": properties.get("sources"),
            "types": properties.get("types"),
            "nst": properties.get("nst"),
            "dmin": float(properties.get("dmin")) if properties.get("dmin") is not None else None,
            "rms": float(properties.get("rms")) if properties.get("rms") is not None else None,
            "gap": float(properties.get("gap")) if properties.get("gap") is not None else None,
            "magType": properties.get("magType"),
            "type": properties.get("type"),
            "title": properties.get("title"),
            "geometry": {
                "longitude": coordinates[0],
                "latitude": coordinates[1],
                "depth": float(coordinates[2]) if coordinates[2] is not None else None
            }
        }
        
        flatten_data.append(flattened_record)
    
    schema = StructType([
        # StructField("id", StringType(), True),
        StructField("place", StringType(), True),
        StructField("mag", FloatType(), True),
        StructField("time", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", FloatType(), True),
        StructField("mmi", FloatType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("geometry", StructType([
            StructField("longitude", FloatType(), True),
            StructField("latitude", FloatType(), True),
            StructField("depth", FloatType(), True)
        ]))
    ])

    return spark.createDataFrame(flatten_data, schema=schema)

def add_column_area(df):
    """
    Add a column to the earthquakes dataframe with the area of the earthquake
    based on its magnitude.
    """  
    add_column_area_df =  df.withColumn("area", split(col("place"),"of").getItem(1))
    
    return add_column_area_df 


    
def write_df_to_gcs_as_json(df, bucket_name, output_path):
    """
    Write the PySpark DataFrame to a GCS bucket as a JSON file.

    :param df: PySpark DataFrame to be written.
    :param bucket_name: GCS bucket name.
    :param output_path: Path (including file name) in GCS where the file will be uploaded.
    """
    try:
        df.write.mode('overwrite').json(output_path)
        print(f"Data successfully written to GCS at {output_path}")
    except Exception as e:
        print(f"Error writing data to GCS: {e}")
        return

def load_df_to_bigquery(df, project_id, dataset_id, table_id, gcs_temp_location):
    """
    Load a DataFrame to BigQuery.
    
    Args:
        df: The DataFrame to load.
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
        gcs_temp_location: GCS bucket for temporary files.
    """
    client = bigquery.Client()
    
    # Define the table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Load DataFrame to BigQuery
    try:
        df.write \
            .format("bigquery") \
            .option("table", table_ref) \
            .option("temporaryGcsBucket", gcs_temp_location) \
            .save()
        print("Data successfully loaded into BigQuery.")
    except Exception as e:
        print(f"Failed to load data into BigQuery: {e}")