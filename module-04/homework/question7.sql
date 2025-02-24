-- Staging Schema add
version: 2

sources:
  - name: staging
    database: "{{ env_var('DBT_DATABASE', 'taxi-rides-ny-448101') }}"
    schema: "{{ env_var('DBT_SCHEMA', 'module_4_dbt') }}"
      # loaded_at_field: record_loaded_at
      # Change DB to now 'module_4_dbt' from trips_data_all
    tables:
      - name: green_tripdata
      - name: fhv_tripdata
      # for homework p#7
      - name: yellow_tripdata
         # freshness:
           # error_after: {count: 6, period: hour}


models:
    - name: stg_fhv_tripdata
      description: >
        Trip made by fhv, also known as for-hire vehicles.
        The records were collected and provided to the NYC Taxi and Limousine Commission (TLC) by
        technology service providers.
      columns:
          - name: dispatching_base_num
            description: dispatching_base_num
          - name: pickup_datetime
            description: The date and time when the meter was engaged.
          - name: dropOff_datetime
            description: The date and time when the meter was disengaged.
          - name: Affiliated_base_numbe
            description: Affiliated_base_numbe
          - name: PUlocationID
            description: locationid where the meter was engaged.
            tests:
              - relationships:
                  to: ref('taxi_zone_lookup')
                  field: locationid
                  severity: warn
          - name: DOlocationID
            description: locationid where the meter was engaged.
            tests:
              - relationships:
                  to: ref('taxi_zone_lookup')
                  field: locationid
          - name: SR_Flag
            description: >
              This flag indicates ?
                Y =
                N =

-- Create stg_fhv_tripdata.sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as
(
  select *,
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("integer")) }} as dispatchid,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("integer")) }} as affilid,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag,

from tripdata
--where rn = 1


-- Create dim_fhv_trips.sql in core
{{
    config(
        materialized='table'
    )
}}

-- join dim_zones and Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12),
-- based on `pickup_datetime`

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS year,
    EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS month,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.tripid,
    fhv_tripdata.dispatchid,
    fhv_tripdata.affilid,
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.SR_Flag,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid


-- Create fct_fhv_monthly_zone_traveltime_p90.sql
WITH trip_dur_perc AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        year,
        month,
        --TIMESTAMP_DIFF(pickup_datetime, dropoff_datetime, SECOND) AS trip_dur_secs,
        PERCENTILE_CONT(TIMESTAMP_DIFF(pickup_datetime, dropoff_datetime, SECOND), 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS p90
    FROM {{ ref('dim_fhv_trips') }}
)

-- Compute the **continous** `p90` of `trip_duration` partitioning by
-- year, month, pickup_location_id, and dropoff_location_id

SELECT * FROM trip_dur_perc

-- Query
SELECT *
FROM `taxi-rides-ny-448101.module_4_dbt_model.fct_fhv_monthly_zone_traveltime_p90`
WHERE pickup_zone IN ('SoHo') AND year = 2019 and month = 11
ORDER BY p90 DESC
LIMIT 100;

-- , 'SoHo', 'Yorkville East'

-- For the Trips that **respectively** started from `Newark Airport`, `SoHo`,
-- and `Yorkville East`, in November 2019, what are **dropoff_zones** with the
-- 2nd longest p90 trip_duration ?






