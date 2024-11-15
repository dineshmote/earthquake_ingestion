import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, udf
from datetime import datetime
import json
import os
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType


# Convert Unix timestamp in milliseconds to GMT
def convert_timestamp_to_gmt(timestamp_ms):
    if timestamp_ms is not None:
        timestamp_s = timestamp_ms / 1000
        return datetime.utcfromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
    return None


# Transform JSON data into a PySpark DataFrame
def transform_data_to_df(spark, json_data):
    features = json_data.get("features", [])
    flatten_data = []

    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]

        flattened_record = {
            "place": properties.get("place"),
            "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
            "time": convert_timestamp_to_gmt(properties.get("time")),
            "updated": convert_timestamp_to_gmt(properties.get("updated")),
            "trl": properties.get("url"),
            "dez": properties.get("tz"),
            "utail": properties.get("detail"),
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
    Add a new column `area` derived from the `place` column.
    """
    extract_area_udf = udf(lambda place: place.split(",")[-1].strip() if place and "," in place else place, StringType())
    return df.withColumn("area", extract_area_udf(col("place")))


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession \
        .builder \
        .appName("Spark Unit Test") \
        .master("local[*]") \
        .getOrCreate()
    return spark


@pytest.fixture
def sample_json_data():
    # File path to the sample JSON
    file_path = r"C:\Brainworks\earthquake_ingestion\test\sample_data.json"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Error: File not found at {file_path}")

    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)  # Return parsed JSON data


@pytest.fixture
def transformed_df(spark_session, sample_json_data):
    return transform_data_to_df(spark_session, sample_json_data)


def test_row_count(transformed_df):
    expected_row_count = 10  # Update this value based on the sample data
    assert transformed_df.count() == expected_row_count, "Row count does not match expected value."


def test_column_existence(transformed_df):
    # 2. Column Check
    expected_columns = {"mag", "place", "time", "updated", "tz", "url", "detail", 
                        "felt", "cdi", "mmi", "alert", "status", "tsunami", 
                        "sig", "net", "code", "ids", "sources", "types", "nst", 
                        "dmin", "rms", "gap", "magType", "type", "title", "geometry"}
    
    actual_columns = set(transformed_df.columns)
    missing_columns = expected_columns - actual_columns

    # Check top-level columns
    assert not missing_columns, f"Missing columns: {missing_columns}"

    # Validate 'geometry' column's subfields
    geometry_schema = transformed_df.schema["geometry"].dataType
    expected_geometry_fields = {"longitude", "latitude", "depth"}
    actual_geometry_fields = {field.name for field in geometry_schema.fields}
    missing_geometry_fields = expected_geometry_fields - actual_geometry_fields

    assert not missing_geometry_fields, f"Missing geometry fields: {missing_geometry_fields}"

def test_column_count(transformed_df):
    # 3. Column Count Check
    # Update the expected column count to match the new schema
    expected_column_count = 27  # Adjust this value based on the updated schema
    assert len(transformed_df.columns) == expected_column_count, \
        f"Column count does not match expected value. Found {len(transformed_df.columns)}, expected {expected_column_count}."

def test_data_type_check(transformed_df):
    # 4. Data Type Check
    expected_schema = {
        "mag": "FloatType",  # Adjusted to match FloatType as per schema
        "place": "StringType",
        "time": "StringType",  # Converted timestamp to StringType
        "updated": "StringType",  # Converted timestamp to StringType
        "tz": "IntegerType",
        "url": "StringType",
        "detail": "StringType",
        "felt": "IntegerType",
        "cdi": "FloatType",
        "mmi": "FloatType",
        "alert": "StringType",
        "status": "StringType",
        "tsunami": "IntegerType",
        "sig": "IntegerType",
        "net": "StringType",
        "code": "StringType",
        "ids": "StringType",
        "sources": "StringType",
        "types": "StringType",
        "nst": "IntegerType",
        "dmin": "FloatType",
        "rms": "FloatType",
        "gap": "FloatType",
        "magType": "StringType",
        "type": "StringType",
        "title": "StringType",
        "geometry": "StructType"  # The 'geometry' column is a StructType containing latitude, longitude, and depth
    }

    actual_schema = {field.name: field.dataType.__class__.__name__ for field in transformed_df.schema.fields}
    
    # Check the top-level schema
    for col_name, data_type in expected_schema.items():
        assert actual_schema.get(col_name) == data_type, \
            f"Data type for column {col_name} does not match. Expected: {data_type}, Found: {actual_schema.get(col_name)}"

    # Validate nested 'geometry' struct schema
    geometry_field = next(field for field in transformed_df.schema.fields if field.name == "geometry")
    expected_geometry_schema = {
        "longitude": "FloatType",
        "latitude": "FloatType",
        "depth": "FloatType"
    }
    actual_geometry_schema = {
        field.name: field.dataType.__class__.__name__ for field in geometry_field.dataType.fields
    }

    # Check the data types of the nested fields inside 'geometry'
    for col_name, data_type in expected_geometry_schema.items():
        assert actual_geometry_schema.get(col_name) == data_type, \
            f"Data type for geometry field {col_name} does not match. Expected: {data_type}, Found: {actual_geometry_schema.get(col_name)}"

def test_add_column_area(spark_session, transformed_df):
    # 5. Add Column Area and Insert Date
    df_with_area = add_column_area(transformed_df)
    df_with_insert_dt = df_with_area.withColumn("insert_dt", current_timestamp())

    # Check if 'area' column is added and not null
    assert "area" in df_with_area.columns, "'area' column not added."
    assert df_with_area.filter(col("area").isNull()).count() == 0, "'area' column contains null values."

    # Check if 'insert_dt' column is added and has timestamps
    assert "insert_dt" in df_with_insert_dt.columns, "'insert_dt' column not added."
    assert df_with_insert_dt.filter(col("insert_dt").isNull()).count() == 0, "'insert_dt' column contains null values."
    
    