#!/usr/bin/env python
# coding: utf-8

import os
import json
import time
import random
import requests
import logging
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from io import BytesIO
import copy

# Configuration
OPENWEATHER_API_KEY = "951d8917fa8b154afd44712c1c73ac4c"  # Votre clé API réelle
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

def fetch_current_weather_data():
    """Fetch real current weather data for New York City to get the structure."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={NYC_LAT}&lon={NYC_LON}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logger.info("Successfully fetched current weather data")
            return response.json()
        else:
            logger.error(f"Failed to fetch weather data: HTTP Status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        return None

def generate_historical_weather_data(template_data, simulation_date):
    """Generate realistic historical weather data based on template and date."""
    if not template_data:
        logger.error("No template data provided")
        return None
    
    # Create a deep copy to avoid modifying the original
    weather_data = copy.deepcopy(template_data)
    
    # Extract month and hour for seasonal and daily variations
    month = simulation_date.month
    hour = simulation_date.hour
    day = simulation_date.day
    
    # Base temperature by month (average NYC temperatures in Celsius)
    base_temps = {
        1: 0.5,    # January
        2: 1.5,    # February
        3: 5.0,    # March
        4: 11.0,   # April
        5: 17.0,   # May
        6: 22.0,   # June
        7: 25.0,   # July
        8: 24.0,   # August
        9: 20.0,   # September
        10: 14.0,  # October
        11: 8.0,   # November
        12: 3.0    # December
    }
    
    # Daily temperature variation (peak at around 2-3pm)
    hour_variation = 5 * pow(math.sin(math.pi * hour / 24), 2) if hour < 15 else 5 * pow(math.sin(math.pi * (24 - hour) / 24), 2)
    
    # Add some randomness for day-to-day variation
    day_variation = random.uniform(-3, 3)
    
    # Calculate temperature
    temperature = base_temps[month] + hour_variation + day_variation
    
    # Humidity tends to be inverse to temperature
    base_humidity = 70 - (temperature / 30) * 15 + random.uniform(-10, 10)
    humidity = max(min(base_humidity, 100), 30)  # Keep between 30% and 100%
    
    # Wind speed (tends to be higher in winter, lower in summer)
    seasonal_factor = 1.5 if month in [11, 12, 1, 2] else 1.0
    wind_speed = random.uniform(0, 8) * seasonal_factor
    
    # Weather conditions based on season and randomness
    weather_conditions = {
        "winter": ["Clear", "Clouds", "Snow", "Mist"],
        "spring": ["Clear", "Clouds", "Rain", "Drizzle", "Thunderstorm"],
        "summer": ["Clear", "Clouds", "Rain", "Thunderstorm"],
        "fall": ["Clear", "Clouds", "Rain", "Drizzle", "Mist"]
    }
    
    # Determine season
    if month in [12, 1, 2]:
        season = "winter"
    elif month in [3, 4, 5]:
        season = "spring"
    elif month in [6, 7, 8]:
        season = "summer"
    else:
        season = "fall"
    
    # Precipitation probability by season
    precipitation_prob = {
        "winter": 0.3,
        "spring": 0.4,
        "summer": 0.3,
        "fall": 0.4
    }
    
    # Decide if it's a precipitation day
    is_precipitation = random.random() < precipitation_prob[season]
    
    # Select weather condition
    if is_precipitation:
        if season == "winter":
            weather_main = random.choice(["Snow", "Clouds"])
            weather_description = "light snow" if weather_main == "Snow" else "overcast clouds"
        else:
            weather_main = random.choice(["Rain", "Drizzle", "Thunderstorm"] if season == "summer" else ["Rain", "Drizzle"])
            weather_description = f"light {weather_main.lower()}"
    else:
        weather_main = random.choice(["Clear", "Clouds"])
        weather_description = "clear sky" if weather_main == "Clear" else "few clouds"
    
    # Map weather condition to category for later use
    if weather_main in ["Clear"]:
        weather_category = "Clear"
    elif weather_main in ["Rain", "Thunderstorm", "Drizzle"]:
        weather_category = "Rainy"
    elif weather_main in ["Snow"]:
        weather_category = "Snowy"
    else:
        weather_category = "Other"
    
    # Update the weather data
    weather_data["dt"] = int(simulation_date.timestamp())
    weather_data["weather"][0]["main"] = weather_main
    weather_data["weather"][0]["description"] = weather_description
    
    # Update main weather parameters
    weather_data["main"]["temp"] = round(temperature, 2)
    weather_data["main"]["feels_like"] = round(temperature - 2 + random.uniform(-2, 2), 2)
    weather_data["main"]["temp_min"] = round(temperature - random.uniform(0, 3), 2)
    weather_data["main"]["temp_max"] = round(temperature + random.uniform(0, 3), 2)
    weather_data["main"]["humidity"] = round(humidity)
    
    # Update wind
    weather_data["wind"]["speed"] = round(wind_speed, 2)
    weather_data["wind"]["deg"] = random.randint(0, 359)
    
    # Update cloud cover
    weather_data["clouds"]["all"] = random.randint(0, 100)
    
    # Add custom fields for our pipeline
    weather_data["simulated"] = True
    weather_data["weather_category"] = weather_category
    weather_data["hour_of_day"] = hour
    weather_data["day_of_week"] = simulation_date.weekday() + 1  # 1 = Monday, 7 = Sunday
    
    return weather_data

def save_weather_data(weather_data, s3_client):
    """Save weather data to MinIO."""
    if weather_data:
        timestamp = datetime.fromtimestamp(weather_data["dt"]).strftime("%Y%m%d_%H%M%S")
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
    
    # Fetch current weather data to use as a template
    template_data = fetch_current_weather_data()
    
    if not template_data:
        logger.error("Failed to get weather template. Cannot proceed with simulation.")
        return
    
    # Save the template for reference
    save_weather_data(template_data, s3_client)
    logger.info("Saved current weather data as template")
    
    # Generate historical data for January 2022 to December 2023
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31, 23, 0, 0)
    
    current_date = start_date
    files_generated = 0
    
    logger.info(f"Starting historical weather data generation from {start_date} to {end_date}")
    
    while current_date <= end_date:
        # Generate historical data based on the template
        weather_data = generate_historical_weather_data(template_data, current_date)
        
        # Save to MinIO
        if weather_data:
            save_weather_data(weather_data, s3_client)
            files_generated += 1
            
            if files_generated % 100 == 0:
                logger.info(f"Generated {files_generated} weather files. Current date: {current_date}")
        
        # Move to next hour
        current_date += timedelta(hours=1)
        
        # Small pause to avoid overwhelming the system
        if files_generated % 500 == 0:
            time.sleep(0.5)
    
    logger.info(f"Weather data generation completed. Total files: {files_generated}")

if __name__ == "__main__":
    import math  # Added for math functions
    main()