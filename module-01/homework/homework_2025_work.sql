--
-- Question 3
SELECT COUNT(*)
FROM green_taxi
WHERE (trip_distance <= 1) AND (lpep_dropoff_datetime >= '2019-10-01') AND (lpep_dropoff_datetime < '2019-11-01')
--
SELECT COUNT(*)
FROM green_taxi
WHERE (trip_distance > 1) AND (trip_distance <= 3) AND (lpep_dropoff_datetime >= '2019-10-01') AND (lpep_dropoff_datetime < '2019-11-01')
--
SELECT COUNT(*)
FROM green_taxi
WHERE (trip_distance > 3) AND (trip_distance <= 7) AND (lpep_dropoff_datetime >= '2019-10-01') AND (lpep_dropoff_datetime < '2019-11-01')
--
SELECT COUNT(*)
FROM green_taxi
WHERE (trip_distance > 7) AND (trip_distance <= 10) AND (lpep_dropoff_datetime >= '2019-10-01') AND (lpep_dropoff_datetime < '2019-11-01')
--
SELECT COUNT(*)
FROM green_taxi
WHERE (trip_distance > 10) AND (lpep_dropoff_datetime >= '2019-10-01') AND (lpep_dropoff_datetime < '2019-11-01')
--
--
-- Question 4
SELECT lpep_pickup_datetime, trip_distance
FROM public.green_taxi
ORDER BY trip_distance DESC
LIMIT 1
--
--
-- Question 5
SELECT z."Zone", SUM(t."total_amount") as amt_sum
FROM green_taxi t JOIN zones z ON z."LocationID" = t."PULocationID"
WHERE (t."lpep_pickup_datetime" > '2019-10-18') AND (t."lpep_pickup_datetime" < '2019-10-19')
GROUP BY z."Zone"
HAVING SUM(t."total_amount") > 13000
ORDER BY amt_sum DESC
--
--
-- Question 6
SELECT zpu."Zone" as zone_up, t."tip_amount", zdo."Zone" as zone_off
FROM green_taxi t JOIN zones zdo ON zdo."LocationID" = t."DOLocationID" JOIN zones zpu ON zpu."LocationID" = t."PULocationID"
WHERE (t."lpep_pickup_datetime" >= '2019-10-01') AND (t."lpep_pickup_datetime" < '2019-11-01') AND zpu."Zone" = 'East Harlem North'
ORDER BY t."tip_amount" DESC
Limit 1

