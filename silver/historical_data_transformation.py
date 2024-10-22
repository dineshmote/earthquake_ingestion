from datetime import datetime
from google.cloud import storage    
from util import read_data_from_gcs, initialize_spark, transform_data_to_df, add_column_area
import os
# def write_df_to_local_parquet(df, local_file_path):
#     """
#     Write the PySpark DataFrame to a local parquet file.
    
#     :param df: PySpark DataFrame to be written.
#     :param local_file_path: Path to the local file where the DataFrame will be written.
#     """
#     df.write.mode("overwrite").parquet(local_file_path)
#     print(f"Data written locally to {local_file_path}")

# def upload_file_to_gcs(local_file_path, bucket_name, gcs_file_path):
#     """
#     Upload a local file to a GCS bucket.
    
#     :param local_file_path: Path to the local file to upload.
#     :param bucket_name: GCS bucket name.
#     :param gcs_file_path: Path (including file name) in GCS where the file will be uploaded.
#     """
#     # Initialize the GCS client
#     client = storage.Client()
#     bucket = client.get_bucket(bucket_name)
#     blob = bucket.blob(gcs_file_path)

#     # Upload the file
#     blob.upload_from_filename(local_file_path)
#     print(f"File {local_file_path} uploaded to GCS at {gcs_file_path}.")
    
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-433112-aecffc0dc374.json"

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
    local_parquet_file = f"earthquake_area_{formatted_date}.parquet"
    gcs_parquet_file = f"processed/earthquake_area_{formatted_date}.parquet"
    
    spark = initialize_spark(app_name)
    
    # Read data back from GCS into PySpark DataFrame
    try:
        json_data = read_data_from_gcs(bucket_name, file_name)
        df = transform_data_to_df(spark, json_data)
        df.show(truncate=False)
        print(f"Total records: {df.count()}")  
    except Exception as e:
        print(f"Error reading data from GCS: {e}")
        return
    
    try:
        add_area_column_df = add_column_area(df)
        add_area_column_df.show(truncate=False)
    except Exception as e:
        print(f"Error adding area column: {e}")
        return
    
    
    try:
        output_path = f"gs://{bucket_name}/processed/earthquake_area"
        add_area_column_df.write.mode('overwrite').parquet(output_path)
        print(f"Data successfully written to GCS at {output_path}")
    except Exception as e:
        print(f"Error writing data to GCS: {e}")
        return
    # try:
    #     add_area_column_df = add_column_area(df)
        
    #     # Step 1: Write DataFrame to local parquet file
    #     write_df_to_local_parquet(add_area_column_df, local_parquet_file)
        
    #     # Step 2: Upload local parquet file to GCS
    #     upload_file_to_gcs(local_parquet_file, bucket_name, gcs_parquet_file)
        
    #     # Optional: Clean up the local file after upload
    #     if os.path.exists(local_parquet_file):
    #         os.remove(local_parquet_file)
    #         print(f"Local file {local_parquet_file} deleted after upload.")
    
    # except Exception as e:
    #     print(f"Error in processing: {e}")

        
    
    
        

if __name__ == "__main__":
    main()
