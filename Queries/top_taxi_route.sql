CREATE TABLE top_taxi_route AS
SELECT
	"PULocationID",
	"DOLocationID",
	count(*) AS trip_count
FROM
	nyc_taxi_data
GROUP BY
	"PULocationID", "DOLocationID"
ORDER BY 
	trip_count DESC
LIMIT 10;

