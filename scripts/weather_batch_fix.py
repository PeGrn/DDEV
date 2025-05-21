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
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
            .config("spark.hadoop.fs.s3a.etag.checksum.enabled", "false") \
            .config("spark.hadoop.fs.s3a.multipart.threshold", "64M") \
            .getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_weather_files_individually(spark):
    """Process each weather file individually to bypass S3A consistency issues."""
    logger.info("Processing weather files individually from MinIO...")
    
    try:
        # Lister tous les fichiers dans le répertoire
        logger.info("Listing all JSON files in weather directory...")
        
        # Utiliser la méthode SQL pour lister les fichiers
        file_list_df = spark.sql("SELECT input_file_name FROM (SELECT 1) CROSS JOIN json.`s3a://nyc-taxi-data/weather/*.json` LIMIT 1")
        
        # Si cette méthode échoue, utiliser l'API FileSystem Java
        if file_list_df.count() == 0:
            logger.info("Using Hadoop FileSystem API to list files...")
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path("s3a://nyc-taxi-data/weather/")
            
            try:
                files = fs.listStatus(path)
                file_list = [file.getPath().toString() for file in files if file.getPath().toString().endswith(".json")]
            except Exception as e:
                logger.error(f"Error listing files: {str(e)}")
                # Méthode de secours avec un modèle prédéfini
                file_list = [f"s3a://nyc-taxi-data/weather/weather_nyc_{y}{m:02d}{d:02d}_{h:02d}0000.json" 
                          for y in range(2022, 2024) 
                          for m in range(1, 13) 
                          for d in range(1, 28) 
                          for h in range(0, 24, 3)]
        else:
            file_list = [row["input_file_name"] for row in file_list_df.collect()]
        
        logger.info(f"Found {len(file_list)} files to process")
        
        # Créer un DataFrame vide avec le schéma attendu
        combined_df = None
        files_processed = 0
        
        # Traiter les fichiers par lots
        batch_size = 50
        
        for i in range(0, len(file_list), batch_size):
            batch_files = file_list[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch_files)} files...")
            
            for file_path in batch_files:
                try:
                    # Lire un seul fichier
                    single_file_df = spark.read.option("mode", "PERMISSIVE").json(file_path)
                    
                    # Effectuer la transformation sur ce fichier
                    transformed_df = transform_weather_file(single_file_df)
                    
                    # Ajouter au DataFrame combiné
                    if combined_df is None:
                        combined_df = transformed_df
                    else:
                        combined_df = combined_df.union(transformed_df)
                    
                    files_processed += 1
                    if files_processed % 20 == 0:
                        logger.info(f"Processed {files_processed} files so far...")
                        
                except Exception as e:
                    logger.warning(f"Error processing file {file_path}: {str(e)}")
                    continue
        
        if combined_df is None or combined_df.count() == 0:
            logger.error("No weather data was processed successfully.")
            return None
        
        logger.info(f"Successfully processed {files_processed} files.")
        return combined_df
        
    except Exception as e:
        logger.error(f"Failed to process weather files: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def transform_weather_file(weather_df):
    """Transform a single weather file."""
    try:
        # Extraire les champs pertinents
        transformed_df = weather_df.select(
            from_unixtime(col("dt")).alias("timestamp"),
            col("main.temp").alias("temperature"),
            col("main.humidity").alias("humidity"),
            col("wind.speed").alias("wind_speed"),
            col("weather").getItem(0).getItem("main").alias("weather_condition"),
            col("weather").getItem(0).getItem("description").alias("weather_description")
        )
        
        # Ajouter des champs dérivés
        transformed_df = transformed_df.withColumn(
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
        
        return transformed_df
    except Exception as e:
        # Tenter une extraction alternative si la première méthode échoue
        try:
            logger.warning(f"Standard transformation failed: {str(e)}. Trying alternative method...")
            
            # Vérifier les colonnes disponibles
            available_cols = weather_df.columns
            
            # Construire une liste dynamique de colonnes à sélectionner
            select_cols = []
            
            # Horodatage
            if "dt" in available_cols:
                select_cols.append(from_unixtime(col("dt")).alias("timestamp"))
            else:
                select_cols.append(lit("2023-01-01").cast("timestamp").alias("timestamp"))
            
            # Température
            if "main" in available_cols and "temp" in weather_df.select("main.*").columns:
                select_cols.append(col("main.temp").alias("temperature"))
            elif "temperature" in available_cols:
                select_cols.append(col("temperature"))
            else:
                select_cols.append(lit(0.0).alias("temperature"))
            
            # Humidité
            if "main" in available_cols and "humidity" in weather_df.select("main.*").columns:
                select_cols.append(col("main.humidity").alias("humidity"))
            elif "humidity" in available_cols:
                select_cols.append(col("humidity"))
            else:
                select_cols.append(lit(0).alias("humidity"))
            
            # Vitesse du vent
            if "wind" in available_cols and "speed" in weather_df.select("wind.*").columns:
                select_cols.append(col("wind.speed").alias("wind_speed"))
            elif "wind_speed" in available_cols:
                select_cols.append(col("wind_speed"))
            else:
                select_cols.append(lit(0.0).alias("wind_speed"))
            
            # Condition météo
            if "weather" in available_cols:
                select_cols.append(col("weather").getItem(0).getItem("main").alias("weather_condition"))
            elif "weather_condition" in available_cols:
                select_cols.append(col("weather_condition"))
            else:
                select_cols.append(lit("Unknown").alias("weather_condition"))
            
            # Description météo
            if "weather" in available_cols:
                select_cols.append(col("weather").getItem(0).getItem("description").alias("weather_description"))
            elif "weather_description" in available_cols:
                select_cols.append(col("weather_description"))
            else:
                select_cols.append(lit("Unknown").alias("weather_description"))
            
            # Sélectionner les colonnes et ajouter les colonnes dérivées
            transformed_df = weather_df.select(*select_cols)
            
            transformed_df = transformed_df.withColumn(
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
            
            return transformed_df
        except Exception as nested_e:
            logger.error(f"Alternative transformation also failed: {str(nested_e)}")
            raise

def save_to_postgres(df, table_name):
    """Save DataFrame to PostgreSQL."""
    logger.info(f"Saving data to PostgreSQL table: {table_name}")
    
    try:
        # Configuration JDBC pour PostgreSQL
        postgres_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        
        # Sauvegarder les données en mode append
        df.write \
            .jdbc(
                url="jdbc:postgresql://postgres:5432/nyc_taxi_db",
                table=table_name,
                mode="append",
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
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Traiter les fichiers météo individuellement
        weather_df = process_weather_files_individually(spark)
        
        if weather_df is None:
            logger.error("No weather data was processed.")
            return
        
        # Compter les résultats
        count = weather_df.count()
        logger.info(f"Processed {count} weather records that will be saved to PostgreSQL")
        
        # Vérifier que le DataFrame n'est pas vide
        if count == 0:
            logger.warning("DataFrame is empty. No data will be saved to PostgreSQL.")
            return
        
        # Afficher un échantillon des données
        logger.info("Sample of data to be saved to PostgreSQL:")
        weather_df.show(5, truncate=False)
        
        # Sauvegarder dans PostgreSQL
        save_to_postgres(weather_df, "dim_weather")
        
        logger.info("Weather batch processing completed successfully")
    except Exception as e:
        logger.error(f"Weather batch processing failed: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Arrêter la session Spark
        spark.stop()

if __name__ == "__main__":
    main()