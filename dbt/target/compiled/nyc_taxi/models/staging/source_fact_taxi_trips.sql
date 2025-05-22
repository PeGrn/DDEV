

-- Échantillonnage stratifié pour conserver la représentativité
WITH taxi_sample AS (
    SELECT 
        *,
        -- Utiliser NTILE pour créer des groupes stratifiés par heure et jour
        NTILE(1000) OVER (
            PARTITION BY pickup_hour, pickup_day_of_week 
            ORDER BY pickup_datetime
        ) as sample_group
    FROM "nyc_taxi_db"."public"."fact_taxi_trips"
    WHERE 
        -- Filtrer sur une période spécifique pour réduire encore plus si nécessaire
        pickup_datetime >= '2022-01-01'
        AND pickup_datetime < '2022-04-01'  -- 3 mois au lieu de 2 ans
),

-- Prendre seulement les 3 premiers groupes de chaque strate (≈ 0.3% des données)
representative_sample AS (
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
    FROM taxi_sample
    WHERE sample_group <= 3  -- Prendre seulement les 3 premiers groupes
)

SELECT * FROM representative_sample