#!/usr/bin/env python
# coding: utf-8

import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, hour, 
    dayofweek, when, lit, to_timestamp, 
    from_unixtime, expr
)
import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Set up logging - Configuration plus détaillée
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session."""
    logger.info("Creating Spark session...")
    # Configuration pour l'accès à MinIO
    os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
    
    try:
        spark = SparkSession.builder \
            .appName("WeatherTransform") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minio") \
            .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_weather_data(spark):
    """
    Process weather data files from MinIO.
    This is a batch process that simulates streaming by processing all files at once.
    """
    logger.info("Processing weather data from MinIO...")
    
    try:
        # Define schema for weather JSON files
        logger.info("Defining schema for weather JSON files...")
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
        
        # List available files in MinIO
        file_pattern = "s3a://nyc-taxi-data/weather/*.json"
        logger.info(f"Loading files matching pattern: {file_pattern}")
        
        # Read all JSON files
        logger.info("Reading JSON files...")
        raw_weather_df = spark.read.json(file_pattern, schema=weather_schema)
        
        # Check if dataframe is empty
        count = raw_weather_df.count()
        logger.info(f"Loaded {count} weather records.")
        
        if count == 0:
            logger.warning("No weather data found. Please make sure the weather files exist in MinIO.")
            return None
        
        # Print sample data
        logger.info("Sample of raw weather data:")
        raw_weather_df.show(2, truncate=False)
        
        # Extract relevant fields and transform
        logger.info("Extracting and transforming relevant fields...")
        weather_df = raw_weather_df.select(
            from_unixtime(col("dt")).alias("timestamp"),
            col("main.temp").alias("temperature"),
            col("main.humidity").alias("humidity"),
            col("wind.speed").alias("wind_speed"),
            col("weather.main").alias("weather_condition"),
            col("weather.description").alias("weather_description")
        )
        
        # Add additional fields
        logger.info("Adding weather category and temporal fields...")
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
        
        # Print sample transformed data
        logger.info("Sample of transformed weather data:")
        weather_df.show(5, truncate=False)
        
        return weather_df
    except Exception as e:
        logger.error(f"Failed to process weather data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def save_to_postgres(df, table_name):
    """Save DataFrame to PostgreSQL."""
    logger.info(f"Saving data to PostgreSQL table: {table_name}")
    
    try:
        # Check if dataframe is empty
        count = df.count()
        logger.info(f"DataFrame has {count} rows to save.")
        
        if count == 0:
            logger.warning("DataFrame is empty. No data will be saved to PostgreSQL.")
            return
        
        logger.info("Preparing JDBC connection properties...")
        postgres_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("Creating PostgreSQL URL...")
        postgres_url = "jdbc:postgresql://postgres:5432/nyc_taxi_db"
        logger.info(f"PostgreSQL URL: {postgres_url}")
        
        # Test connection to PostgreSQL
        logger.info("Testing connection to PostgreSQL...")
        test_df = df.limit(1)
        
        logger.info("Starting write operation to PostgreSQL...")
        df.write \
            .jdbc(
                url=postgres_url,
                table=table_name,
                mode="overwrite",
                properties=postgres_properties
            )
        
        logger.info(f"Successfully saved {count} rows to PostgreSQL table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to save data to PostgreSQL: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("PostgreSQL connection details:")
        logger.error(f"  URL: jdbc:postgresql://postgres:5432/nyc_taxi_db")
        logger.error(f"  Table: {table_name}")
        logger.error(f"  User: postgres")
        raise

def test_postgres_connection():
    """Test connection to PostgreSQL directly."""
    logger.info("Testing direct connection to PostgreSQL...")
    
    try:
        from py4j.java_gateway import JavaGateway
        gateway = JavaGateway()
        
        # Create a Java connection to PostgreSQL
        logger.info("Creating JDBC connection...")
        conn = gateway.jvm.java.sql.DriverManager.getConnection(
            "jdbc:postgresql://postgres:5432/nyc_taxi_db", 
            "postgres", 
            "postgres"
        )
        
        logger.info("Successfully connected to PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main ETL process."""
    logger.info("Starting ETL process for weather data...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Test PostgreSQL connection
        logger.info("Testing PostgreSQL connection...")
        connection_successful = test_postgres_connection()
        logger.info(f"PostgreSQL connection test result: {'SUCCESS' if connection_successful else 'FAILED'}")
        
        # Process weather data
        logger.info("Processing weather data from MinIO...")
        weather_df = process_weather_data(spark)
        
        if weather_df is None or weather_df.count() == 0:
            logger.error("No weather data was processed. Cannot save to PostgreSQL.")
            return
        
        # Save to PostgreSQL
        logger.info("Saving weather data to PostgreSQL...")
        save_to_postgres(weather_df, "dim_weather")
        
        logger.info("Weather data processing completed successfully")
    except Exception as e:
        logger.error(f"Weather data processing failed: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    logger.info("Script started")
    try:
        # Global variable for spark session to be used across functions
        spark = create_spark_session()
        main()
    except Exception as e:
        logger.error(f"Uncaught exception: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)