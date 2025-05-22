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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session."""
    logger.info("Creating Spark session in local mode...")
    # Configuration pour l'accès à MinIO
    os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
    
    try:
        spark = SparkSession.builder \
            .appName("WeatherBatchTransform") \
            .master("local[*]") \
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
    """Process all weather data files from MinIO in batch mode."""
    logger.info("Processing all weather data from MinIO in batch mode...")
    
    # Read all JSON files
    file_pattern = "s3a://nyc-taxi-data/weather/*.json"
    logger.info(f"Loading files matching pattern: {file_pattern}")
    
    try:
        raw_weather_df = spark.read.json(file_pattern)
        
        # Check if dataframe is empty
        count = raw_weather_df.count()
        logger.info(f"Loaded {count} weather records.")
        
        if count == 0:
            logger.warning("No weather data found.")
            return None
        
        # Remove duplicates based on timestamp to avoid processing the same data multiple times
        logger.info("Removing duplicates based on timestamp...")
        raw_weather_df = raw_weather_df.dropDuplicates(["dt"])
        
        deduplicated_count = raw_weather_df.count()
        logger.info(f"After deduplication: {deduplicated_count} unique weather records.")
        
        # Print schema to debug
        logger.info("Raw weather data schema:")
        raw_weather_df.printSchema()
        
        # Show a sample of the data
        logger.info("Sample of raw weather data:")
        raw_weather_df.show(2, truncate=False)
        
        # Extract relevant fields and transform
        logger.info("Extracting and transforming relevant fields...")
        
        # Handle different possible schema structures
        try:
            # Try first using the expected schema
            weather_df = raw_weather_df.select(
                from_unixtime(col("dt")).alias("timestamp"),
                col("main.temp").alias("temperature"),
                col("main.humidity").alias("humidity"),
                col("wind.speed").alias("wind_speed"),
                col("weather").getItem(0).getItem("main").alias("weather_condition"),
                col("weather").getItem(0).getItem("description").alias("weather_description")
            )
            
            # Test if it worked by checking if columns exist
            weather_df.select("temperature").limit(1).collect()
            
        except Exception as e:
            logger.warning(f"First schema extraction attempt failed: {str(e)}")
            
            # Try alternative schema based on the actual data
            try:
                # Check for simulated field to determine schema source
                if "simulated" in raw_weather_df.columns:
                    weather_df = raw_weather_df.select(
                        from_unixtime(col("dt")).alias("timestamp"),
                        col("main.temp").alias("temperature"),
                        col("main.humidity").alias("humidity"),
                        col("wind.speed").alias("wind_speed"),
                        col("weather_category").alias("weather_category"),
                        col("weather").getItem(0).getItem("main").alias("weather_condition"),
                        col("weather").getItem(0).getItem("description").alias("weather_description")
                    )
                else:
                    # Try a completely different schema
                    weather_df = raw_weather_df.select(
                        col("timestamp").alias("timestamp"),
                        col("temp").alias("temperature"),
                        col("humidity").alias("humidity"),
                        col("wind_speed").alias("wind_speed"),
                        col("weather_main").alias("weather_condition"),
                        col("weather_description").alias("weather_description")
                    )
            except Exception as e2:
                logger.error(f"Alternative schema extraction also failed: {str(e2)}")
                # Last resort: dynamically determine columns
                logger.info("Trying dynamic column detection...")
                
                # Get all column names
                all_cols = raw_weather_df.columns
                logger.info(f"Available columns: {all_cols}")
                
                # Build a dataframe with best-effort column mapping
                select_expr = []
                
                if "dt" in all_cols:
                    select_expr.append(from_unixtime(col("dt")).alias("timestamp"))
                elif "timestamp" in all_cols:
                    select_expr.append(col("timestamp").alias("timestamp"))
                else:
                    # Use current timestamp as fallback
                    select_expr.append(lit("2023-01-01 00:00:00").cast("timestamp").alias("timestamp"))
                
                # Temperature
                if "main" in all_cols and raw_weather_df.select("main").first()[0] is not None:
                    select_expr.append(col("main.temp").alias("temperature"))
                elif "temp" in all_cols:
                    select_expr.append(col("temp").alias("temperature"))
                else:
                    select_expr.append(lit(0.0).alias("temperature"))
                
                # Humidity
                if "main" in all_cols and raw_weather_df.select("main").first()[0] is not None:
                    select_expr.append(col("main.humidity").alias("humidity"))
                elif "humidity" in all_cols:
                    select_expr.append(col("humidity").alias("humidity"))
                else:
                    select_expr.append(lit(0).alias("humidity"))
                
                # Wind speed
                if "wind" in all_cols and raw_weather_df.select("wind").first()[0] is not None:
                    select_expr.append(col("wind.speed").alias("wind_speed"))
                elif "wind_speed" in all_cols:
                    select_expr.append(col("wind_speed").alias("wind_speed"))
                else:
                    select_expr.append(lit(0.0).alias("wind_speed"))
                
                # Weather condition
                if "weather" in all_cols and raw_weather_df.select("weather").first()[0] is not None:
                    select_expr.append(col("weather").getItem(0).getItem("main").alias("weather_condition"))
                elif "weather_main" in all_cols:
                    select_expr.append(col("weather_main").alias("weather_condition"))
                elif "weather_condition" in all_cols:
                    select_expr.append(col("weather_condition").alias("weather_condition"))
                else:
                    select_expr.append(lit("Unknown").alias("weather_condition"))
                
                # Weather description
                if "weather" in all_cols and raw_weather_df.select("weather").first()[0] is not None:
                    select_expr.append(col("weather").getItem(0).getItem("description").alias("weather_description"))
                elif "weather_description" in all_cols:
                    select_expr.append(col("weather_description").alias("weather_description"))
                else:
                    select_expr.append(lit("Unknown").alias("weather_description"))
                
                # Create dataframe with the determined columns
                weather_df = raw_weather_df.select(*select_expr)
        
        # Add additional fields
        logger.info("Adding weather category and temporal fields...")
        if "weather_category" in weather_df.columns:
            # Weather category already exists
            weather_df = weather_df.withColumn(
                "hour_of_day", hour(col("timestamp"))
            ).withColumn(
                "day_of_week", dayofweek(col("timestamp"))
            )
        else:
            # Need to create weather category
            weather_df = weather_df.withColumn(
                "weather_category",
                when(col("weather_condition") == "Clear", "Clear")
                .when(col("weather_condition").isin("Rain", "Drizzle", "Thunderstorm"), "Rainy")
                .when(col("weather_condition") == "Snow", "Snowy")
                .otherwise("Other")
            ).withColumn(
                "hour_of_day", hour(col("timestamp"))
            ).withColumn(
                "day_of_week", dayofweek(col("timestamp"))
            )
        
        # Show sample transformed data
        logger.info("Sample of transformed weather data:")
        weather_df.show(5, truncate=False)
        
        return weather_df
    except Exception as e:
        logger.error(f"Failed to process weather data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def save_to_postgres(df, table_name):
    """Save DataFrame to PostgreSQL using append mode only (since we cleaned the table manually)."""
    logger.info(f"Saving data to PostgreSQL table: {table_name}")
    
    try:
        # Configuration JDBC
        postgres_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        
        # Simply append data (table will be created if it doesn't exist)
        df.write \
            .jdbc(
                url="jdbc:postgresql://postgres:5432/nyc_taxi_db",
                table=table_name,
                mode="append",  # Use append mode
                properties=postgres_properties
            )
        
        logger.info(f"Successfully saved data to PostgreSQL table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to save data to PostgreSQL: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def main():
    """Main ETL process."""
    logger.info("Starting batch processing of weather data...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process all weather data in batch mode
        weather_df = process_weather_data(spark)
        
        if weather_df is None or weather_df.count() == 0:
            logger.error("No weather data was processed.")
            return
        
        # Count the results
        count = weather_df.count()
        logger.info(f"Processed {count} weather records that will be saved to PostgreSQL")
        
        # Expected count validation
        expected_count = 17520  # 2 years * 365 days * 24 hours
        if count != expected_count:
            logger.warning(f"Expected {expected_count} records but got {count}. This might indicate duplicate data or missing files.")
        else:
            logger.info(f"Perfect! Got exactly {expected_count} records as expected.")
        
        # Save to PostgreSQL
        save_to_postgres(weather_df, "dim_weather")
        
        logger.info("Weather batch processing completed successfully")
    except Exception as e:
        logger.error(f"Weather batch processing failed: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()