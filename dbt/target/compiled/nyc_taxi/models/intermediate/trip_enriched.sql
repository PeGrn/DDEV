

WITH taxi_trips AS (
    SELECT *
    FROM "nyc_taxi_db"."public"."source_fact_taxi_trips"
),

weather AS (
    SELECT *
    FROM "nyc_taxi_db"."public"."source_dim_weather"
),

-- Créer une jointure simple basée sur l'heure et le jour
weather_simplified AS (
    SELECT 
        hour_of_day,
        day_of_week,
        AVG(temperature) as avg_temperature,
        AVG(humidity) as avg_humidity,
        AVG(wind_speed) as avg_wind_speed,
        -- Utiliser la première valeur non-null pour les catégories
        MIN(weather_condition) as weather_condition,
        MIN(weather_category) as weather_category
    FROM weather
    WHERE temperature IS NOT NULL 
      AND hour_of_day IS NOT NULL 
      AND day_of_week IS NOT NULL
    GROUP BY hour_of_day, day_of_week
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
    
    -- Données météo jointées
    w.avg_temperature as temperature,
    w.avg_humidity as humidity,
    w.avg_wind_speed as wind_speed,
    w.weather_condition,
    w.weather_category
FROM
    taxi_trips t
LEFT JOIN
    weather_simplified w
    -- Jointure simple sur heure et jour de la semaine
    ON t.pickup_hour = w.hour_of_day 
    AND t.pickup_day_of_week = w.day_of_week