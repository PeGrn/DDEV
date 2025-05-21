#!/usr/bin/env python
# coding: utf-8
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, when, lit, 
    round as spark_round, datediff, 
    to_timestamp, from_unixtime
)
import logging
from pyspark.sql.types import DoubleType

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session."""
    # Configuration pour l'accès à MinIO
    os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
    
    spark = SparkSession.builder \
        .appName("YellowTaxiTransform") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    return spark

def load_taxi_data(spark, year):
    """Load Yellow Taxi data from MinIO."""
    # MinIO connection details
    access_key = "minio"
    secret_key = "minio123"
    
    # Set S3A configuration
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Read parquet files
    taxi_df = spark.read.parquet(f"s3a://nyc-taxi-data/yellow_taxi/yellow_tripdata_{year}-*.parquet")
    return taxi_df

def transform_taxi_data(df):
    """
    Transform Yellow Taxi data.
    Extract:
    - Trip duration (dropoff - pickup)
    - Distance buckets: 0-2km, 2-5km, >5km
    - Payment type (lookup table)
    - Tip percentage (tip_amount / fare_amount)
    - Pickup hour, day of week
    """
    # Create a payment type lookup dictionary
    payment_types = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    
    # Convert kilometers to miles (approximation)
    km_to_miles = 0.621371
    
    # Register the dictionary as a temporary view
    payment_df = spark.createDataFrame(
        [(k, v) for k, v in payment_types.items()], 
        ["payment_type_id", "payment_type_desc"]
    )
    payment_df.createOrReplaceTempView("payment_types")
    
    # Transform the data
    transformed_df = df.withColumn("trip_duration_minutes", 
                                  ((col("tpep_dropoff_datetime").cast("long") - 
                                    col("tpep_pickup_datetime").cast("long")) / 60)) \
        .withColumn("distance_km", col("trip_distance") / km_to_miles) \
        .withColumn("distance_bucket", 
                   when(col("distance_km") <= 2, "0-2 km")
                   .when((col("distance_km") > 2) & (col("distance_km") <= 5), "2-5 km")
                   .otherwise(">5 km")) \
        .withColumn("tip_percentage", 
                   when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100)
                   .otherwise(0.0)) \
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
    
    # Join with payment types
    transformed_df = transformed_df.join(
        payment_df,
        transformed_df.payment_type == payment_df.payment_type_id,
        "left"
    )
    
    return transformed_df

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

def prepare_fact_taxi_trips(df):
    """Prepare final fact_taxi_trips table."""
    # Select only the needed columns for the fact table
    fact_df = df.select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("trip_duration_minutes"),
        col("distance_km"),
        col("distance_bucket"),
        col("fare_amount"),
        col("tip_amount"),
        col("tip_percentage"),
        col("payment_type_desc").alias("payment_type"),
        col("pickup_hour"),
        col("pickup_day_of_week"),
        col("passenger_count"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id")
    )
    
    # Remove any invalid data
    fact_df = fact_df.filter(
        (col("trip_duration_minutes") > 0) &
        (col("distance_km") > 0) &
        (col("fare_amount") >= 0)
    )
    
    return fact_df

def main():
    """Main ETL process."""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        logger.info("Loading Yellow Taxi data from MinIO")
        taxi_df = load_taxi_data(spark, 2022)
        
        # Transform data
        logger.info("Transforming taxi data")
        transformed_df = transform_taxi_data(taxi_df)
        
        # Prepare fact table
        logger.info("Preparing fact_taxi_trips table")
        fact_df = prepare_fact_taxi_trips(transformed_df)
        
        # Save to PostgreSQL
        logger.info("Saving data to PostgreSQL")
        save_to_postgres(fact_df, "fact_taxi_trips")
        
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    spark = create_spark_session()
    main()