CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);



           
CREATE TABLE green_trips (
    pickup_datetime  	TIMESTAMP,
    dropoff_datetime 	TIMESTAMP,
    pickup_location_id 	INTEGER,
    dropoff_location_id	INTEGER,
    passenger_count     INTEGER,
    trip_distance       NUMERIC(10,2),
    tip_amount          NUMERIC(10,2),
    total_amount        NUMERIC(10,2)
);


select count(1) from green_trips;

select * from green_trips where passenger_count  is null;

select count(1) from green_trips where trip_distance >5;



CREATE TABLE homework_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    passenger_count INTEGER,
    tip_amount NUMERIC(10,2),
    event_timestamp TIMESTAMP
)

create TABLE homework_processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
)


select count(1) from homework_processed_events_aggregated;

SELECT PULocationID, num_trips
FROM homework_processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 3;

select sum(num_trips) from homework_processed_events_aggregated;

create TABLE homework_session_processed_events_aggregated (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start,window_end, PULocationID)
)

select PULocationID, hspea.num_trips  from homework_session_processed_events_aggregated hspea
order by hspea.num_trips  desc limit 5;




DROP TABLE IF EXISTS homework_session_processed_events_aggregated;

CREATE TABLE homework_session_processed_events_aggregated (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    window_time BIGINT,
    pulocationid INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end, pulocationid)
);






SELECT window_start, window_end,window_time , pulocationid, num_trips, total_revenue
FROM homework_session_processed_events_aggregated
ORDER BY homework_session_processed_events_aggregated.num_trips desc, window_start  ;


select sum(num_trips) from homework_session_processed_events_aggregated;



select count(*) from green_trips gt 
where gt.pickup_location_id = 74 and gt.pickup_datetime  between
'2025-10-08 06:46:14.000' and	'2025-10-08 08:27:40.000'
	

select * from green_trips gt 
where gt.pickup_location_id = 74 and gt.pickup_datetime  between
'2025-10-08 06:46:14.000' and	'2025-10-08 08:27:40.000'


SELECT
    gt.*,
    gt.pickup_datetime
      - LAG(gt.pickup_datetime) OVER (
            PARTITION BY gt.pickup_location_id
            ORDER BY gt.pickup_datetime
        ) AS diff_from_prev
FROM green_trips gt
WHERE gt.pickup_location_id = 74
  AND gt.pickup_datetime BETWEEN
	'2025-10-08 06:46:14.000' and	'2025-10-08 08:27:40.000'
ORDER BY gt.pickup_datetime;


SELECT
    window_start,
    window_end,
    pulocationid,
    num_trips,
    total_revenue
FROM homework_session_processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 20;


SELECT
    gt.pickup_datetime,
    gt.pickup_location_id,
    gt.pickup_datetime
      - LAG(gt.pickup_datetime) OVER (
            PARTITION BY gt.pickup_location_id
            ORDER BY gt.pickup_datetime
        ) AS diff_from_prev
FROM green_trips gt
WHERE gt.pickup_location_id = 74
  AND gt.pickup_datetime BETWEEN '2025-10-08 06:46:14.000' and	'2025-10-08 08:27:40.000'

  
  CREATE TABLE homework_tumble_processed_events_aggregated (
    window_start TIMESTAMP(3),
    num_trips BIGINT,
    tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);

