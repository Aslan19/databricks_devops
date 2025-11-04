-- Please edit the sample below

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue 
COMMENT "Daily summary of NYC taxi trips including revenue and distance metrics"
AS
SELECT
  TO_DATE(pickup_datetime) AS trip_date,
  COUNT(*) AS total_trips,
  SUM(passenger_count) AS total_passengers,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_revenue,
  ROUND(AVG(trip_distance), 2) AS avg_distance
FROM LIVE.nyc_taxi_silver
GROUP BY TO_DATE(pickup_datetime)
ORDER BY trip_date;

CREATE OR REFRESH MATERIALIZED VIEW gold_payment_type_summary 
COMMENT "Daily revenue and tip summary grouped by payment type"
AS
SELECT
  TO_DATE(pickup_datetime) AS trip_date,
  payment_type,
  COUNT(*) AS total_trips,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(SUM(tip_amount), 2) AS total_tips
FROM LIVE.nyc_taxi_silver
GROUP BY TO_DATE(pickup_datetime), payment_type;

CREATE OR REFRESH MATERIALIZED VIEW gold_vendor_performance 
COMMENT "Daily performance metrics per vendor including revenue, tips, and distance"
AS
SELECT
  vendor_id,
  TO_DATE(pickup_datetime) AS trip_date,
  COUNT(*) AS total_trips,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(SUM(tip_amount), 2) AS total_tips,
  ROUND(AVG(trip_distance), 2) AS avg_distance
FROM LIVE.nyc_taxi_silver
GROUP BY vendor_id, TO_DATE(pickup_datetime);
