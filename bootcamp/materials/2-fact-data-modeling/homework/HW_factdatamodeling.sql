# Week 2 Fact Data Modeling

- A query to deduplicate `game_details` from Day 1 so there's no duplicates
SELECT * FROM 
    (
		SELECT *, row_number() OVER (PARTITION BY player_id, game_id) AS row_num
    	FROM game_details
	) deduped 
	WHERE row_num = 1 

- A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE user_devices_cumulated (
	user_id DECIMAL,
	browser_type TEXT,
	device_activity_datelist DATE[],
	PRIMARY KEY (user_id,browser_type)
)

- A cumulative query to generate `device_activity_datelist` derived from `events`

WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
),
today AS (
	SELECT e.user_id,
			d.browser_type,
			ARRAY_AGG(DISTINCT DATE(e.event_time) ORDER BY DATE(e.event_time)) as device_activity_datelist
			
	FROM events e
	JOIN (SELECT DISTINCT device_id, browser_type FROM devices) d
	ON e.device_id = d.device_id 
	WHERE e.user_id IS NOT NULL
	GROUP BY e.user_id, d.browser_type
	ORDER BY e.user_id, d.browser_type
)

INSERT INTO user_devices_cumulated
SELECT
    COALESCE(t.user_id, y.user_id) as user_id,
    COALESCE(t.browser_type, y.browser_type) as browser_type,
    CASE 
        WHEN y.device_activity_datelist IS NULL THEN t.device_activity_datelist
        WHEN t.device_activity_datelist IS NOT NULL THEN 
            (SELECT ARRAY_AGG(DISTINCT dates ORDER BY dates)
             FROM (
                 SELECT UNNEST(y.device_activity_datelist || t.device_activity_datelist) as dates
             ) unique_dates)
        ELSE y.device_activity_datelist
    END as device_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id 
    AND t.browser_type = y.browser_type


- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

SELECT 
    user_id,
    browser_type,
    ARRAY_AGG(
        CAST(EXTRACT(EPOCH FROM date_value)::INTEGER / 86400 AS INTEGER)
        ORDER BY date_value
    ) as datelist_int
FROM (
    SELECT 
        user_id,
        browser_type,
        UNNEST(device_activity_datelist) as date_value
    FROM user_devices_cumulated
) as unnested
GROUP BY user_id, browser_type


- A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity

  CREATE TABLE hosts_cumulated (
	host VARCHAR,
	host_activity_datelist DATE[],
	PRIMARY KEY (host)
)
  
- The incremental query to generate `host_activity_datelist`

WITH yesterday AS (
    SELECT * FROM hosts_cumulated
),
today AS (
    SELECT 
        host,
        ARRAY_AGG(DISTINCT DATE(event_time) ORDER BY DATE(event_time)) as host_activity_datelist
    FROM events
    WHERE DATE(event_time) = CURRENT_DATE
    GROUP BY host
)

INSERT INTO hosts_cumulated
SELECT
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN t.host_activity_datelist
        WHEN t.host_activity_datelist IS NOT NULL THEN 
            (SELECT ARRAY_AGG(DISTINCT dates ORDER BY dates)
             FROM (
                 SELECT UNNEST(y.host_activity_datelist || t.host_activity_datelist) as dates
             ) unique_dates)
        ELSE y.host_activity_datelist
    END as host_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
    ON t.host = y.host

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

CREATE TABLE host_activity_reduced (
    month DATE,
    host VARCHAR,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (month, host)
)

- An incremental query that loads `host_activity_reduced`
  - day-by-day

WITH yesterday AS (
    SELECT * FROM host_activity_reduced
),
today AS (
    SELECT 
        DATE_TRUNC('month', event_time::timestamp)::DATE as month,
        host,
        ARRAY[COUNT(1)] as hit_array,
        ARRAY[COUNT(DISTINCT user_id)] as unique_visitors
    FROM events 
    WHERE DATE(event_time) = CURRENT_DATE
    GROUP BY DATE_TRUNC('month', event_time::timestamp), host
)

INSERT INTO host_activity_reduced
SELECT
    COALESCE(t.month, y.month) as month,
    COALESCE(t.host, y.host) as host,
    CASE
        WHEN y.hit_array IS NULL THEN t.hit_array
        WHEN t.hit_array IS NOT NULL THEN 
            y.hit_array || t.hit_array
        ELSE y.hit_array
    END as hit_array,
    CASE
        WHEN y.unique_visitors IS NULL THEN t.unique_visitors
        WHEN t.unique_visitors IS NOT NULL THEN
            y.unique_visitors || t.unique_visitors
        ELSE y.unique_visitors
    END as unique_visitors
FROM today t
FULL OUTER JOIN yesterday y
    ON t.month = y.month 
    AND t.host = y.host