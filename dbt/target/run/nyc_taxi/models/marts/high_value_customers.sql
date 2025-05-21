
  
    

  create  table "nyc_taxi_db"."public"."high_value_customers__dbt_tmp"
  as (
    

WITH trip_data AS (
    SELECT *
    FROM "nyc_taxi_db"."public"."trip_enriched"
),

customer_stats AS (
    SELECT
        -- Using passenger_count as a proxy for customer identification
        -- In a real scenario, you might have a proper customer ID
        passenger_count,
        pickup_location_id,
        COUNT(*) AS num_trips,
        SUM(fare_amount + tip_amount) AS total_spent,
        AVG(tip_percentage) AS avg_tip_percentage
    FROM
        trip_data
    WHERE
        passenger_count > 0
    GROUP BY
        passenger_count,
        pickup_location_id
)

SELECT
    passenger_count,
    pickup_location_id,
    num_trips,
    total_spent,
    avg_tip_percentage
FROM
    customer_stats
WHERE
    num_trips > 10
    AND total_spent > 300
    AND avg_tip_percentage > 15
  );
  