from pyspark.sql import SparkSession
from google.cloud import storage
import os
import json
import requests
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, LongType, FloatType


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Brainworks\GCP\gcp-data-project-433112-b6d2c0754752.json"

def initialize_spark(app_name):
    """
    Initialize PySpark session.
    
    :param app_name: Name of the Spark application.
    :return: SparkSession object.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    return spark

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
    # Initialize GCS client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Convert the data to JSON string and upload it to GCS
    json_data = json.dumps(data)
    blob.upload_from_string(json_data, content_type='application/json')
    print(f"Data written to GCS bucket {bucket_name} as {file_name}.")

def read_data_from_gcs(spark, bucket_name, file_name):
    """
    Read JSON data from GCS into a PySpark DataFrame using GCS Client Libraries.
    
    :param spark: SparkSession object.
    :param bucket_name: GCS bucket name.
    :param file_name: Name of the file to read from the GCS bucket.
    :return: DataFrame containing the data read from GCS.
    """
    # Initialize GCS client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Download the JSON data as a string
    data = blob.download_as_string()
    json_data = json.loads(data)
    
    # Extract relevant features
    features = json_data.get("features", [])
    
    flatten_data = []
    
    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]
        
        flattened_record = {
            "id": properties.get("id"),
            "place": properties.get("place"),
            "mag": float(properties.get("mag")) ,
            "time": properties.get("time"),
            "updated": properties.get("updated"),
            "tz": properties.get("tz"),
            "url": properties.get("url"),
            "detail": properties.get("detail"),
            "felt": properties.get("felt"),
            "cdi": float(properties.get("cdi")),
            "mmi": float(properties.get("mmi")),
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
            "dmin": float(properties.get("dmin")),
            "rms": float(properties.get("rms")),
            "gap": float(properties.get("gap")),
            "magType": properties.get("magType"),
            "type": properties.get("type"),
            "title": properties.get("title"),
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth": float(coordinates[2])
        }
        
        flatten_data.append(flattened_record)
    
    # Define the schema
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("place", StringType(), True),
        StructField("mag", FloatType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
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
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True)
    ])
    
    # Create a DataFrame from the flattened data
    df = spark.createDataFrame(flatten_data, schema=schema)
    
    return df


def main():
    """
    Main function to handle the flow of reading API data, saving it to GCS, 
    and reading it back into PySpark.
    """
    # Configuration
    current_date = datetime.now()
    formatted_date = current_date.strftime('%Y%m%d')
    app_name = "APIDataToGCS"
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    bucket_name = "earthquake_analysis_data_bucket"
    file_name = f"pyspark/landing/{formatted_date}/earthquake_raw.json"
    
    spark = initialize_spark(app_name)
    
    # Fetch data from API
    try:
        data = fetch_data_from_api(api_url)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    
    # Write data to GCS using the client library
    try:
        write_data_to_gcs(data, bucket_name, file_name)
    except Exception as e:
        print(f"Error writing data to GCS: {e}")
        return
    
    # Read data back from GCS into PySpark DataFrame
    try:
        df = read_data_from_gcs(spark, bucket_name, file_name)
        df.show(truncate=False)
        print(df.count())
        
    except Exception as e:
        print(f"Error reading data from GCS: {e}")
        return

if __name__ == "__main__":
    main()
