{{ config(materialized='view') }}

SELECT
    timestamp,
    temperature,
    humidity,
    wind_speed,
    weather_condition,
    weather_description,
    weather_category,
    hour_of_day,
    day_of_week
FROM
    {{ source('nyc_taxi', 'dim_weather') }}