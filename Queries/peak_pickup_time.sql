SELECT
	EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
	COUNT(*) AS trip_count
FROM
	nyc_taxi_data
GROUP BY
	pickup_hour
ORDER BY
	trip_count DESC