SELECT
	count(*)
FROM
	green_trip_data
WHERE (green_trip_data.lpep_pickup_datetime BETWEEN '2019-01-15 00:00:00' AND '2019-01-16 00:00:00')


SELECT
	date_trunc('day', lpep_pickup_datetime) AS pickup_day,
	MAX(trip_distance) AS max_distance
FROM
	green_trip_data
GROUP BY
	date_trunc('day', lpep_pickup_datetime)
ORDER BY max_distance DESC


SELECT
	passenger_count,
	count(passenger_count)
FROM
	green_trip_data
WHERE (green_trip_data.lpep_pickup_datetime BETWEEN '2019-01-01 00:00:00'
	AND '2019-01-02 00:00:00')
	AND (passenger_count=2 OR passenger_count=3)
GROUP BY passenger_count


SELECT
	puzones. "Zone" AS puzone,
	dozones. "Zone" AS dozone,
	green_trip_data.tip_amount as tip,
	green_trip_data. "PULocationID",
	green_trip_data. "DOLocationID"
FROM
	green_trip_data
	INNER JOIN taxi_zone_lookup AS puzones ON green_trip_data. "PULocationID" = puzones. "LocationID"
	LEFT JOIN taxi_zone_lookup AS dozones ON green_trip_data. "DOLocationID" = dozones. "LocationID"

WHERE
	puzones. "Zone" = 'Astoria'
ORDER BY
	tip DESC