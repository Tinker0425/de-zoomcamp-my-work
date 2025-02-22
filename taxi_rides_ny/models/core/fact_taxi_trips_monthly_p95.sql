WITH filtered_data AS (
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
        AND payment_type_description IN ('Cash', 'Credit Card')
)

SELECT
    service_type,
    year,
    month,
    APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97,
    APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95,
    APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90
FROM filtered_data
GROUP BY service_type, year, month
ORDER BY year, month, service_type
