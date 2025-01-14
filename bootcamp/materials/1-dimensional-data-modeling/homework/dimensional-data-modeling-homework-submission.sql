SELECT * FROM actor_films

/* Create a DDL for an actors table */
CREATE TYPE films AS (
	film TEXT,
	year INTEGER,
	votes INTEGER,
	rating INTEGER,
	filmid INTEGER
)

CREATE TYPE quality_class AS ENUM('star','good','avg','bad');

CREATE TABLE actors (
		actor TEXT,
		actorid INTEGER,
		is_active BOOLEAN,
		films films[],
		quality_class quality_class,
		current_year INTEGER,
		PRIMARY KEY (actorid,current_year)
)

/* Cumulative table generation query: 
Write a query that populates the actors table one year at a time */ 
WITH yesterday AS (
	SELECT *
	FROM actor_films
	WHERE year = 1969
),
	today AS (
	SELECT *
	FROM actor_films
	WHERE year = 1970
)
INSERT INTO actors
SELECT 
	COALESCE(t.actor,y.actor) AS actor,
	COALESCE(t.actorid,y.actorid) AS actorid,
	t.year IS NOT NULL AS is_active,
	CASE WHEN y.year IS NULL 
		THEN ARRAY[ROW(
					t.film,
					t.year,
					t.votes,
					t.rating,
					t.filmid
		)::films]
	WHEN t.year IS NOT NULL
		THEN y.films || ARRAY[ROW(
					t.film,
					t.year,
					t.votes,
					t.rating,
					t.filmid
		)::films]
	ELSE y.films
	END AS films,
	CASE WHEN t.year IS NOT NULL THEN 
		CASE WHEN t.rating>8 THEN 'star'
			WHEN t.rating>7 THEN 'good'
			WHEN t.rating>6 THEN 'avg'
			ELSE 'bad' 
			END::quality_class
		ELSE y.quality_class
	END as quality_class,
	COALESCE(t.year,y.current_year+1) as current_year

	FROM yesterday y
	FULL OUTER JOIN today t
	ON y.actorid = t.actorid  

/* DDL for actors_history_scd table: 
Create a DDL for an actors_history_scd table */
CREATE TABLE players_scd_table
(
	actor TEXT,
	quality_class scoring_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER
)

/* Backfill query for actors_history_scd: 
Write a "backfill" query that can populate 
the entire actors_history_scd table in a single query */
INSERT INTO players_scd_table
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

/* Incremental query for actors_history_scd: 
Write an "incremental" query that combines the previous year's SCD data 
with new incoming data from the actors table */

