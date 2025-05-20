#!/usr/bin/env python
# coding: utf-8

import os
import json
import time
import requests
import logging
from datetime import datetime
import boto3
from botocore.client import Config
from io import BytesIO

# Configuration
OPENWEATHER_API_KEY = "951d8917fa8b154afd44712c1c73ac4c"  # Replace with your API key
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
BUCKET_NAME = 'nyc-taxi-data'
FOLDER_NAME = 'weather'

# New York City coordinates
NYC_LAT = 40.7128
NYC_LON = -74.0060

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_minio_client():
    """Create and return a MinIO client."""
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    return s3_client

def ensure_bucket_exists(s3_client, bucket_name):
    """Ensure that the bucket exists, create it if it doesn't."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except:
        logger.info(f"Creating bucket {bucket_name}")
        s3_client.create_bucket(Bucket=bucket_name)

def fetch_weather_data():
    """Fetch current weather data for New York City."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={NYC_LAT}&lon={NYC_LON}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch weather data: HTTP Status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        return None

def save_weather_data(weather_data, s3_client):
    """Save weather data to MinIO."""
    if weather_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"weather_nyc_{timestamp}.json"
        
        # Convert JSON to bytes
        json_data = json.dumps(weather_data).encode('utf-8')
        
        # Upload to MinIO
        object_key = f"{FOLDER_NAME}/{file_name}"
        s3_client.upload_fileobj(
            BytesIO(json_data),
            BUCKET_NAME,
            object_key
        )
        logger.info(f"Weather data saved to MinIO: {file_name}")
        return file_name
    return None

def main():
    # Create MinIO client
    s3_client = create_minio_client()
    
    # Ensure bucket exists
    ensure_bucket_exists(s3_client, BUCKET_NAME)
    
    # Fetch and save weather data
    weather_data = fetch_weather_data()
    if weather_data:
        save_weather_data(weather_data, s3_client)
    
    logger.info("Weather data fetch completed")

if __name__ == "__main__":
    main()