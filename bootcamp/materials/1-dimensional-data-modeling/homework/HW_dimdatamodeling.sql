CREATE TYPE films AS (
	film TEXT,
	year INTEGER,
	votes INTEGER,
	rating REAL,
	filmid TEXT
)

CREATE TYPE quality_class AS ENUM('star','good','avg','bad')

CREATE TABLE actors (
		actorid TEXT,
		films films[],
		quality_class quality_class,
		is_active BOOLEAN,
		PRIMARY KEY (actorid)
)

-- Cumulative table generation query
-- Write a query that populates the actors table one year at a time 
WITH actor_ratings AS (
    SELECT
        actorid,
        array_agg(ROW(film, year, votes, rating, filmid)::films ORDER BY year DESC) AS films,  
        MAX(year) AS most_recent_year,                                           
        SUM(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE) THEN 1 ELSE 0 END) > 0 AS is_active  
    FROM actor_films
    GROUP BY actorid
),
actor_quality AS (
    SELECT
        ar.actorid,
        ar.films,
        ar.most_recent_year,
        ar.is_active,
        CASE 
            WHEN AVG(af.rating) > 8 THEN 'star'
            WHEN AVG(af.rating) > 7 THEN 'good'
            WHEN AVG(af.rating) > 6 THEN 'avg'
            ELSE 'bad'
        END::quality_class AS quality_class
    FROM actor_films af
    JOIN actor_ratings ar ON ar.actorid = af.actorid
    WHERE af.year = ar.most_recent_year  
    GROUP BY ar.actorid, ar.films, ar.most_recent_year, ar.is_active
)
-- Insert data into the actors table
INSERT INTO actors 
SELECT
    actorid,
    films,
    quality_class,
    is_active
FROM actor_quality

--Create a DDL for an `actors_history_scd` table implementing type 2 dimensional modeling
CREATE TABLE actors_history_scd (
    actorid TEXT,                   
    actor TEXT,        
    films films,  
    quality_class quality_class,     
    is_active BOOLEAN,             
    start_date DATE,               
    end_date DATE,                 
    last_updated TIMESTAMP,  
    PRIMARY KEY (actorid, start_date)  
)

-- Insert historical records for all actors
INSERT INTO actors_history_scd
SELECT 
    actor,
    quality_class,
    is_active,
    current_year as start_date,
    LEAD(current_year) OVER (
        PARTITION BY actorid 
        ORDER BY current_year
    ) - 1 as end_date,
    current_year
FROM (
    SELECT 
        actor,
        actorid,
        quality_class,
        is_active,
        current_year,
        LAG(quality_class) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) as prev_quality,
        LAG(is_active) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) as prev_active
    FROM actors
) changes
WHERE 
    quality_class IS DISTINCT FROM prev_quality OR
    is_active IS DISTINCT FROM prev_active OR
    prev_quality IS NULL

-- Incrementally add new data from actors table
INSERT INTO actors_history_scd
SELECT 
    actor,
    quality_class,
    is_active,
    current_year as start_date,
    NULL as end_date,
    current_year
FROM actors a
WHERE current_year = (SELECT MAX(current_year) FROM actors)
AND EXISTS (
    SELECT 1 
    FROM actors_history_scd h
    WHERE h.actor = a.actor
    AND (h.quality_class != a.quality_class OR h.is_active != a.is_active)
    AND h.end_date IS NULL
)

UPDATE actors_history_scd
SET end_date = (SELECT MAX(current_year) - 1 FROM actors)
WHERE end_date IS NULL 
AND actor IN (
    SELECT h.actor
    FROM actors_history_scd h
    JOIN actors a ON h.actor = a.actor
    WHERE a.current_year = (SELECT MAX(current_year) FROM actors)
    AND (h.quality_class != a.quality_class OR h.is_active != a.is_active)
)



