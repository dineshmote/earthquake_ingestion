from datetime import datetime
from google.cloud import storage  
from pyspark.sql.functions import current_timestamp  
from util import read_data_from_gcs, initialize_spark, transform_data_to_df, add_column_area, write_df_to_gcs_as_json, load_df_to_bigquery
import os
    
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-433112-aecffc0dc374.json"

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
    
    # Read data back from GCS into PySpark DataFrame
    try:
        json_data = read_data_from_gcs(bucket_name, file_name)
        df = transform_data_to_df(spark, json_data)
        df.show(truncate=False)
        print(f"Total records: {df.count()}")  
    except Exception as e:
        print(f"Error reading data from GCS: {e}")
        return

    # Add area column to the DataFrame
    try:
        add_area_column_df = add_column_area(df)
        add_area_column_df.show(truncate=False)
    except Exception as e:
        print(f"Error adding area column: {e}")
        return
    
    # Write the updated DataFrame to GCS as a JSON file
    try:
        output_path = f"gs://{bucket_name}/pyspark/silver/{formatted_date}/earthquake_silver.json"
        write_df_to_gcs_as_json(add_area_column_df, bucket_name, output_path)
        print(f"Data successfully written to GCS at {output_path}")
    except Exception as e:
        print(f"Error writing data to GCS: {e}")
        return
    
    # Add insert_dt column (current timestamp)
    try:
        final_df = add_area_column_df.withColumn("insert_dt", current_timestamp())
        final_df.show(truncate=False)
    except Exception as e:
        print(f"Error adding insert_dt column: {e}")
        return

    # Load data into BigQuery
    project_id = "gcp-data-project-433112"  
    dataset_id = "earthquake_ingestion"
    table_id = "earthquake_data"  
    gcs_temp_location = "earthquake_analysis_data_bucket"
    
     # Write the DataFrame to BigQuery
    try:
        load_df_to_bigquery(final_df, project_id, dataset_id, table_id, gcs_temp_location)
    except Exception as e:
        print(f"Error writing data to BigQuery: {e}")
        return
    
    
    
        

if __name__ == "__main__":
    main()
