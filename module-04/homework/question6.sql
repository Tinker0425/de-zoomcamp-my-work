SELECT *
FROM (
  SELECT
    EXTRACT(YEAR FROM pickup_datetime) as year,
    EXTRACT(MONTH FROM pickup_datetime) as month,
    service_type,
    PERCENTILE_CONT(fare_amount, 0.97) OVER(PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) as p97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER(PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) as p95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER(PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) as p90
  FROM
    taxi-rides-ny-448101.module_4_dbt_model.fact_trips
  WHERE
    fare_amount > 0
    AND trip_distance > 0
    AND lower(payment_type_description) in ('cash', 'credit card')
    AND pickup_datetime >= '2020-04-01' and pickup_datetime < '2020-05-01' and service_type = 'Yellow'
  ) AS new_table
--GROUP BY service_type, year, month
ORDER BY p97, p95, p90