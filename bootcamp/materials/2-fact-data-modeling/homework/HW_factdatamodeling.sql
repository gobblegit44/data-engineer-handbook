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

- A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
  
- The incremental query to generate `host_activity_datelist`

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

- An incremental query that loads `host_activity_reduced`
  - day-by-day