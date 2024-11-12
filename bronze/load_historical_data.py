from datetime import datetime
from util import fetch_data_from_api, write_data_to_gcs, read_data_from_gcs, initialize_spark, transform_data_to_df, add_column_area
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-440907-eb61e9727efa.json"

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
    bucket_name = "earthquake_analysis_data1"
    file_name = f"pyspark/landing/{formatted_date}/earthquake_raw.json"
    
    spark = initialize_spark(app_name)
    
    # Fetch data from API
    try:
        data = fetch_data_from_api(api_url)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    
    # Write data to GCS
    try:
        write_data_to_gcs(data, bucket_name, file_name)
    except Exception as e:
        print(f"Error writing data to GCS: {e}")
        return

        

if __name__ == "__main__":
    main()
