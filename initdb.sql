-- Create database
CREATE DATABASE nyc_taxi_db;

-- Connect to the database
\c nyc_taxi_db

-- Create tables
CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_duration_minutes FLOAT,
    distance_km FLOAT,
    distance_bucket VARCHAR(10),
    fare_amount FLOAT,
    tip_amount FLOAT,
    tip_percentage FLOAT,
    payment_type VARCHAR(20),
    pickup_hour INT,
    pickup_day_of_week INT,
    passenger_count INT,
    pickup_location_id INT,
    dropoff_location_id INT
);

CREATE TABLE IF NOT EXISTS dim_weather (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity INT,
    wind_speed FLOAT,
    weather_condition VARCHAR(50),
    weather_description VARCHAR(100),
    weather_category VARCHAR(20),
    hour_of_day INT,
    day_of_week INT
);

-- Create database for Airflow
CREATE DATABASE airflow;