#!/usr/bin/env python
# coding: utf-8
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, when, lit,
    round as spark_round, datediff,
    to_timestamp, from_unixtime, rand
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
        .appName("YellowTaxiTransformFixed") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    return spark

def load_taxi_data_sample(spark, year, sample_fraction=0.01):
    """Load Yellow Taxi data from MinIO with sampling."""
    logger.info(f"Loading taxi data for {year} with {sample_fraction*100}% sample")
    
    # Set S3A configuration
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "minio")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minio123")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Read only first 3 months to reduce data volume
    taxi_df = spark.read.parquet(f"s3a://nyc-taxi-data/yellow_taxi/yellow_tripdata_{year}-0[1-3].parquet")
    
    # Apply stratified sampling to maintain representativeness
    sampled_df = taxi_df.withColumn("rand", rand(seed=42)) \
        .filter(col("rand") < sample_fraction) \
        .drop("rand")
    
    original_count = taxi_df.count()
    sampled_count = sampled_df.count()
    
    logger.info(f"Original data: {original_count:,} records")
    logger.info(f"Sampled data: {sampled_count:,} records ({sampled_count/original_count*100:.2f}%)")
    
    return sampled_df

def transform_taxi_data(df):
    """Transform Yellow Taxi data with FIXED hour/day extraction."""
    
    # Create a payment type lookup dictionary
    payment_types = {
        1: "Credit card",
        2: "Cash",
        3: "No charge", 
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    
    # Convert kilometers to miles
    km_to_miles = 0.621371
    
    # Register payment types as a temporary view
    spark = df.sql_ctx.sparkSession
    payment_df = spark.createDataFrame(
        [(k, v) for k, v in payment_types.items()],
        ["payment_type_id", "payment_type_desc"]
    )
    payment_df.createOrReplaceTempView("payment_types")
    
    # Transform the data - FIX: Ensure pickup_datetime is properly parsed
    transformed_df = df \
        .filter(col("tpep_pickup_datetime").isNotNull()) \
        .filter(col("tpep_dropoff_datetime").isNotNull()) \
        .withColumn("trip_duration_minutes",
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
    
    # Show sample to debug
    logger.info("Sample of transformed data (checking hour/day calculation):")
    sample_data = transformed_df.select(
        "tpep_pickup_datetime", "pickup_hour", "pickup_day_of_week", "fare_amount"
    ).limit(5)
    sample_data.show(truncate=False)
    
    # Join with payment types
    transformed_df = transformed_df.join(
        payment_df,
        transformed_df.payment_type == payment_df.payment_type_id,
        "left"
    )
    
    return transformed_df

def save_to_postgres(df, table_name):
    """Save DataFrame to PostgreSQL using TRUNCATE + INSERT to avoid dependency issues."""
    postgres_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    # Show column info before saving
    logger.info("Columns being saved to PostgreSQL:")
    logger.info(f"Schema: {df.schema}")
    
    try:
        # SOLUTION: Manually truncate the table first via SQL connection
        logger.info(f"Truncating table {table_name} to avoid dependency conflicts...")
        
        # Create a connection to execute TRUNCATE
        import subprocess
        truncate_command = f"""
        docker exec nyc_taxi_postgres psql -U postgres -d nyc_taxi_db -c "TRUNCATE TABLE {table_name} CASCADE;"
        """
        
        # Execute the truncate command
        result = subprocess.run(truncate_command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Successfully truncated table {table_name}")
        else:
            logger.warning(f"Could not truncate table (might not exist yet): {result.stderr}")
        
        # Now use append mode to insert data
        logger.info(f"Inserting data into {table_name} using append mode...")
        df.write \
            .jdbc(
                url="jdbc:postgresql://postgres:5432/nyc_taxi_db",
                table=table_name,
                mode="append",  # CHANGED: Use append instead of overwrite
                properties=postgres_properties
            )
        
        logger.info(f"Data successfully saved to PostgreSQL table: {table_name}")
        
    except Exception as e:
        logger.error(f"Error during save operation: {str(e)}")
        
        # Fallback: try direct append (will add to existing data)
        logger.info("Trying direct append as fallback...")
        try:
            df.write \
                .jdbc(
                    url="jdbc:postgresql://postgres:5432/nyc_taxi_db",
                    table=table_name,
                    mode="append",
                    properties=postgres_properties
                )
            
            logger.info(f"Data appended to PostgreSQL table: {table_name}")
        except Exception as fallback_error:
            logger.error(f"Fallback append also failed: {str(fallback_error)}")
            raise fallback_error

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
        (col("trip_duration_minutes") < 300) &  # Remove trips > 5 hours
        (col("distance_km") > 0) &
        (col("distance_km") < 100) &  # Remove trips > 100km
        (col("fare_amount") >= 0) &
        (col("fare_amount") < 500) &  # Remove fares > $500
        (col("pickup_hour").isNotNull()) &  # Ensure hour is not null
        (col("pickup_day_of_week").isNotNull())  # Ensure day is not null
    )
    
    # Debug: show sample of final data
    logger.info("Sample of final fact table data:")
    fact_df.select("pickup_datetime", "pickup_hour", "pickup_day_of_week", "fare_amount").show(5)
    
    return fact_df

def main():
    """Main ETL process with sampling."""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load sampled data (1% of Q1 2022)
        logger.info("Loading sampled Yellow Taxi data from MinIO")
        taxi_df = load_taxi_data_sample(spark, 2022, sample_fraction=0.01)
        
        # Transform data
        logger.info("Transforming taxi data")
        transformed_df = transform_taxi_data(taxi_df)
        
        # Prepare fact table
        logger.info("Preparing fact_taxi_trips table")
        fact_df = prepare_fact_taxi_trips(transformed_df)
        
        # Show statistics
        final_count = fact_df.count()
        logger.info(f"Final processed records: {final_count:,}")
        
        if final_count == 0:
            logger.error("No records to save! Check data filtering conditions.")
            return
        
        # Save to PostgreSQL
        logger.info("Saving data to PostgreSQL")
        save_to_postgres(fact_df, "fact_taxi_trips")
        
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()