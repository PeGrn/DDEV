{{ config(materialized='view') }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    distance_km,
    distance_bucket,
    fare_amount,
    tip_amount,
    tip_percentage,
    payment_type,
    pickup_hour,
    pickup_day_of_week,
    passenger_count,
    pickup_location_id,
    dropoff_location_id
FROM
    {{ source('nyc_taxi', 'fact_taxi_trips') }}