import os
import requests
import pandas as pd
from datetime import datetime
import logging
from io import BytesIO
import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
BUCKET_NAME = 'nyc-taxi-data'
FOLDER_NAME = 'yellow_taxi'

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

def download_taxi_data(years_and_months, s3_client):
    """
    Download yellow taxi data for specified years and months and upload to MinIO.
    years_and_months is a dictionary where keys are years and values are lists of months.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    for year, months in years_and_months.items():
        for month in months:
            month_str = str(month).zfill(2)
            file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
            url = f"{base_url}/{file_name}"
            
            logger.info(f"Downloading {file_name} from {url}")
            
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    # Upload directly to MinIO
                    object_key = f"{FOLDER_NAME}/{file_name}"
                    s3_client.upload_fileobj(
                        BytesIO(response.content),
                        BUCKET_NAME,
                        object_key
                    )
                    logger.info(f"Successfully uploaded {file_name} to MinIO")
                else:
                    logger.error(f"Failed to download {file_name}: HTTP Status {response.status_code}")
            except Exception as e:
                logger.error(f"Error downloading {file_name}: {str(e)}")
            
            # Add a small delay to avoid overwhelming the server
            import time
            time.sleep(1)

def main():
    # Create MinIO client
    s3_client = create_minio_client()
    
    # Ensure bucket exists
    ensure_bucket_exists(s3_client, BUCKET_NAME)
    
    # Define years and months to download (January 2022 to December 2023)
    years_and_months = {
        2022: range(1, 13),  # All months of 2022
        2023: range(1, 13)   # All months of 2023
    }

    
    # Download the data
    download_taxi_data(years_and_months, s3_client)
    
    logger.info("Taxi data download completed for 2022-2023")

if __name__ == "__main__":
    main()