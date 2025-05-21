

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
    "nyc_taxi_db"."public"."dim_weather"