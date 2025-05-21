

WITH trip_data AS (
    SELECT *
    FROM "nyc_taxi_db"."public"."trip_enriched"
)

SELECT
    pickup_hour,
    weather_category,
    COUNT(*) AS num_trips,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(fare_amount) AS avg_fare_amount,
    AVG(distance_km) AS avg_distance
FROM
    trip_data
GROUP BY
    pickup_hour,
    weather_category