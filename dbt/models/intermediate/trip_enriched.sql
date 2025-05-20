{{ config(materialized='table') }}

WITH taxi_trips AS (
    SELECT *
    FROM {{ ref('source_fact_taxi_trips') }}
),

weather AS (
    SELECT *
    FROM {{ ref('source_dim_weather') }}
)

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.trip_duration_minutes,
    t.distance_km,
    t.distance_bucket,
    t.fare_amount,
    t.tip_amount,
    t.tip_percentage,
    t.payment_type,
    t.pickup_hour,
    t.pickup_day_of_week,
    t.passenger_count,
    t.pickup_location_id,
    t.dropoff_location_id,
    w.temperature,
    w.humidity,
    w.wind_speed,
    w.weather_condition,
    w.weather_category
FROM
    taxi_trips t
LEFT JOIN
    weather w
    -- Join on the closest weather observation by hour
    ON t.pickup_hour = w.hour_of_day
    AND t.pickup_day_of_week = w.day_of_week