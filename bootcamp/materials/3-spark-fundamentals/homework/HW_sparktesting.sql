--PostgreSQL to SparkSQL conversion:
-- Create a struct type for films
CREATE STRUCT films (
    film STRING,
    year INT,
    votes INT, 
    rating FLOAT,
    filmid STRING
);

-- Create actors table using Delta format
CREATE TABLE actors (
    actorid STRING,
    films ARRAY<STRUCT<film:STRING, year:INT, votes:INT, rating:FLOAT, filmid:STRING>>,
    quality_class STRING,
    is_active BOOLEAN
) USING DELTA;

-- Cumulative table generation query
WITH actor_ratings AS (
    SELECT 
        actorid,
        collect_list(
            named_struct(
                'film', film,
                'year', year,
                'votes', votes,
                'rating', rating,
                'filmid', filmid
            )
        ) AS films,
        MAX(year) AS most_recent_year,
        SUM(CASE WHEN year = year(current_date()) THEN 1 ELSE 0 END) > 0 AS is_active
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
        END AS quality_class
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