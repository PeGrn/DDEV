
  create view "nyc_taxi_db"."public"."source_fact_taxi_trips__dbt_tmp" as (
    

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
    "nyc_taxi_db"."public"."fact_taxi_trips"
  );