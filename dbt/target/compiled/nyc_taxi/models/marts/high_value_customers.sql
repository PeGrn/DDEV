

WITH trip_data AS (
    SELECT *
    FROM "nyc_taxi_db"."public"."trip_enriched"
    WHERE fare_amount IS NOT NULL 
      AND tip_amount IS NOT NULL
      AND passenger_count IS NOT NULL
      AND passenger_count > 0
),

customer_stats AS (
    SELECT
        -- Utiliser passenger_count et pickup_location_id comme proxy pour identifier les "clients"
        passenger_count,
        pickup_location_id,
        COUNT(*) AS num_trips,
        SUM(fare_amount + COALESCE(tip_amount, 0)) AS total_spent,
        AVG(COALESCE(tip_percentage, 0)) AS avg_tip_percentage
    FROM trip_data
    GROUP BY passenger_count, pickup_location_id
    HAVING COUNT(*) > 0  -- Au moins 1 trip pour éviter les divisions par zéro
)

SELECT
    passenger_count,
    pickup_location_id,
    num_trips,
    -- Utiliser CAST au lieu de ROUND pour PostgreSQL
    CAST(total_spent AS DECIMAL(10,2)) as total_spent,
    CAST(avg_tip_percentage AS DECIMAL(10,2)) as avg_tip_percentage,
    
    -- Critères ajustés pour l'échantillon
    CASE 
        WHEN num_trips >= 5 AND total_spent >= 100 AND avg_tip_percentage >= 12 
        THEN 'High Value Customer'
        ELSE 'Regular Customer'
    END as customer_category
FROM customer_stats
WHERE 
    num_trips >= 2  -- Ajusté pour l'échantillon (au lieu de 10)
    AND total_spent >= 50  -- Ajusté pour l'échantillon (au lieu de 300)
    AND avg_tip_percentage >= 10  -- Ajusté pour l'échantillon (au lieu de 15)
ORDER BY total_spent DESC