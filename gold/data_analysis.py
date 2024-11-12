from datetime import datetime, timedelta
from google.cloud import storage  
from pyspark.sql.functions import col, count , avg, to_date, lit
from pyspark.sql import SparkSession
# from util import read_data_from_gcs, initialize_spark, transform_data_to_df, add_column_area, write_df_to_gcs_as_json, read_parquet_from_gcs_bucket, upload_dataframe_to_gcs_as_parquet, load_df_to_bigquery
import os
    
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Dinesh Mote\Downloads\gcp-data-project-440907-eb61e9727efa.json"

def main():
    """
    Main function to handle the flow of reading API data, saving it to GCS, 
    and reading it back into PySpark.
    """
    spark = SparkSession.builder \
        .appName("earthquake_analysis") \
        .getOrCreate()

    try:
      final_df = spark.read.format("bigquery").load("gcp-data-project-440907.earthquake_ingestion.earthquake_data")
    #   final_df.show(truncate=False)
      final_df.printSchema()
    except Exception as e:
        print("Error while creating final df") 

    #1. Count the number of earthquakes by region

    earthquake_counts_by_area = final_df.filter(col("type") == "earthquake") \
                    .groupBy("area")\
                    .agg(count("*").alias("earthquake_count"))
        
    earthquake_counts_by_area.show()

    #2. Find the average magnitude by the region

    avg_mag_df = final_df.groupBy("area")\
                .agg(avg("mag").alias("avg_mag"))
    avg_mag_df.show(truncate= False)

    #3. Find how many earthquakes happen on the same day
    earthquakes_per_day = final_df.withColumn("earthquake_date", to_date("time")) \
                    .groupBy("earthquake_date") \
                    .agg(count("*").alias("earthquake_count"))
    earthquakes_per_day.show(truncate= False)

    #4. Find how many earthquakes happen on same day and in same region
    earthquakes_per_day_region = final_df.withColumn("earthquake_date", to_date("time"))\
                    .groupBy("earthquake_date", "area")\
                    .agg(count("*").alias("earthquake_count"))
    earthquakes_per_day_region.show(truncate= False)

    #5. Find average earthquakes happen on the same day
    daily_earthquake_counts = final_df.withColumn("earthquake_date", to_date("time"))\
                    .groupBy("earthquake_date")\
                    .agg(count("*").alias("num_of_earthquakes"))
    daily_earthquake = daily_earthquake_counts.groupBy("earthquake_date")\
                    .agg(avg("num_of_earthquakes"))
                    
    average_daily_earthquakes = daily_earthquake_counts.agg(avg("num_of_earthquakes").alias("average_earthquakes_per_day"))
    average_daily_earthquakes.show(truncate= False)

    #6. Find average earthquakes happen on same day a nd in same region
    daily_region_earthquake_counts = final_df.withColumn("earthquake_date", to_date("time"))\
                    .groupBy("earthquake_date", "area")\
                    .agg(count("*").alias("num_of_earthquakes"))
                    
    average_daily_region_earthquakes = daily_region_earthquake_counts.groupBy("earthquake_date", "area")\
                    .agg(avg("num_of_earthquakes").alias("average_earthquakes_per_day_per_region"))
    average_daily_region_earthquakes.show(truncate= False)

    #7. Find the region name, which had the highest magnitude earthquake last week
    current_date = datetime.now()
    last_week_date = current_date - timedelta(days=7)
    highest_magnitude_last_week = final_df.filter(
                        (to_date("time") >= lit(last_week_date.strftime("%Y-%m-%d"))) &
                        (to_date("time") <= lit(current_date.strftime("%Y-%m-%d")))
                    )\
                    .orderBy(col("mag").desc())\
                    .select("area")\
                    .limit(1)
    highest_magnitude_last_week.show(truncate= False)

    #8. Find the region name, which is having magnitudes higher than 5
    print("8. Region name, which is having magnitudes higher than 5")

    region_with_higher_mag_df = final_df.filter(col("mag")> 5)\
                    .select("area", "mag")
    region_with_higher_mag_df.show(truncate= False)

    #9. Find out the regions which are having the highest frequency and intensity of earthquakes
    print("9. The regions which are having the highest frequency and intensity of earthquakes")

    highest_frequency_intensity_region = final_df.groupBy("area")\
                  .agg(count("*").alias("frequency"),avg("mag").alias("intensity"))\
                  .orderBy(col("frequency").desc(), col("intensity").desc())\
                  .limit(1)
    highest_frequency_intensity_region.show(truncate= False)




        
if __name__ == "__main__":
    main()
