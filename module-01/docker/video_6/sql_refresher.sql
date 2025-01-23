-- sql WHERE
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "drop_off_loc"
FROM
	yellow_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID"=zpu."LocationID" AND
	t."DOLocationID"=zdo."LocationID"
LIMIT 100;


-- same, but using JOIN
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "drop_off_loc"
FROM
	yellow_taxi_data t JOIN zones zpu
		ON t."PULocationID"=zpu."LocationID"
	JOIN zones zdo
		ON t."DOLocationID"=zdo."LocationID"
LIMIT 100;



-- GROUP BY 1 item
SELECT
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	COUNT(1) as "count",
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_data t
GROUP BY
	CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "count" DESC;


-- GROUP BY multi
SELECT
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	"DOLocationID",
	COUNT(1) as "count",
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_data t
GROUP BY
	1, 2
ORDER BY "day" ASC, "DOLocationID" ASC;
