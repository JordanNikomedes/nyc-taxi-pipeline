SELECT
	EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
	AVG(trip_distance) AS avg_trip_distance
FROM
	nyc_taxi_data
GROUP BY
	pickup_hour
ORDER BY
	pickup_hour;