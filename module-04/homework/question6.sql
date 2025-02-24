-- in Cloud IDE DBT (AI support)
WITH clean_fact_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount,
        trip_distance,
        payment_type_description
    FROM {{ ref('fact_trips') }}
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND lower(payment_type_description) in ('cash', 'credit card')
),

fare_amt_perc AS(
    SELECT
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
    FROM clean_fact_trips
    --GROUP BY service_type, year, month
    --ORDER BY year, month, service_type
)

SELECT * FROM fare_amt_perc



-- In BigQuery
SELECT service_type, year, month, p97, p95, p90
FROM `taxi-rides-ny-448101.module_4_dbt_model.fact_taxi_trips_monthly_p95`
WHERE year = 2020 and month = 4
GROUP BY service_type, year, month, p97, p95, p90