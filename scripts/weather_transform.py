#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, hour, 
    dayofweek, when, lit, to_timestamp, 
    from_unixtime, expr
)
import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName("WeatherTransform") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    return spark

def process_weather_data(spark):
    """
    Process weather data files from MinIO.
    This is a batch process that simulates streaming by processing all files at once.
    """
    # MinIO connection details
    access_key = "minio"
    secret_key = "minio123"
    
    # Set S3A configuration
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Define schema for weather JSON files
    weather_schema = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType()),
            StructField("lat", DoubleType())
        ])),
        StructField("weather", StructType([
            StructField("id", IntegerType()),
            StructField("main", StringType()),
            StructField("description", StringType()),
            StructField("icon", StringType())
        ])),
        StructField("base", StringType()),
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("feels_like", DoubleType()),
            StructField("temp_min", DoubleType()),
            StructField("temp_max", DoubleType()),
            StructField("pressure", IntegerType()),
            StructField("humidity", IntegerType())
        ])),
        StructField("visibility", IntegerType()),
        StructField("wind", StructType([
            StructField("speed", DoubleType()),
            StructField("deg", IntegerType()),
            StructField("gust", DoubleType())
        ])),
        StructField("clouds", StructType([
            StructField("all", IntegerType())
        ])),
        StructField("dt", IntegerType()),
        StructField("sys", StructType([
            StructField("type", IntegerType()),
            StructField("id", IntegerType()),
            StructField("country", StringType()),
            StructField("sunrise", IntegerType()),
            StructField("sunset", IntegerType())
        ])),
        StructField("timezone", IntegerType()),
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("cod", IntegerType())
    ])
    
    # Read all JSON files
    raw_weather_df = spark.read.json("s3a://nyc-taxi-data/weather/*.json", schema=weather_schema)
    
    # Extract relevant fields and transform
    weather_df = raw_weather_df.select(
        from_unixtime(col("dt")).alias("timestamp"),
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        col("wind.speed").alias("wind_speed"),
        col("weather.main").alias("weather_condition"),
        col("weather.description").alias("weather_description")
    )
    
    # Add additional fields
    weather_df = weather_df.withColumn(
        "weather_category",
        when(col("weather_condition") == "Clear", "Clear")
        .when(col("weather_condition") == "Rain", "Rainy")
        .when(col("weather_condition") == "Thunderstorm", "Stormy")
        .otherwise("Other")
    ).withColumn(
        "hour_of_day", hour(col("timestamp"))
    ).withColumn(
        "day_of_week", dayofweek(col("timestamp"))
    )
    
    return weather_df

def save_to_postgres(df, table_name):
    """Save DataFrame to PostgreSQL."""
    postgres_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    df.write \
        .jdbc(
            url="jdbc:postgresql://postgres:5432/nyc_taxi_db",
            table=table_name,
            mode="overwrite",
            properties=postgres_properties
        )
    
    logger.info(f"Data saved to PostgreSQL table: {table_name}")

def main():
    """Main ETL process."""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process weather data
        logger.info("Processing weather data from MinIO")
        weather_df = process_weather_data(spark)
        
        # Save to PostgreSQL
        logger.info("Saving weather data to PostgreSQL")
        save_to_postgres(weather_df, "dim_weather")
        
        logger.info("Weather data processing completed successfully")
    except Exception as e:
        logger.error(f"Weather data processing failed: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()