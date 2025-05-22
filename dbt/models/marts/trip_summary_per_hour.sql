{{ config(materialized='table') }}

WITH trip_data AS (
    SELECT *
    FROM {{ ref('trip_enriched') }}
    WHERE pickup_hour IS NOT NULL
      AND trip_duration_minutes IS NOT NULL
      AND fare_amount IS NOT NULL
)

SELECT
    pickup_hour,
    COALESCE(weather_category, 'Unknown') as weather_category,
    COUNT(*) AS num_trips,
    -- Utiliser CAST au lieu de ROUND pour PostgreSQL
    CAST(AVG(trip_duration_minutes) AS DECIMAL(10,2)) AS avg_trip_duration,
    CAST(AVG(COALESCE(tip_percentage, 0)) AS DECIMAL(10,2)) AS avg_tip_percentage,
    CAST(AVG(fare_amount) AS DECIMAL(10,2)) AS avg_fare_amount,
    CAST(AVG(COALESCE(distance_km, 0)) AS DECIMAL(10,2)) AS avg_distance,
    
    -- Statistiques supplémentaires
    MIN(fare_amount) as min_fare,
    MAX(fare_amount) as max_fare,
    CAST(STDDEV(fare_amount) AS DECIMAL(10,2)) as stddev_fare
FROM trip_data
GROUP BY pickup_hour, COALESCE(weather_category, 'Unknown')
HAVING COUNT(*) >= 1  -- Au moins 1 trip par groupe
ORDER BY pickup_hour, num_trips DESC