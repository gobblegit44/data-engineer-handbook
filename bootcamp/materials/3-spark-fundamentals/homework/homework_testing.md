# PySpark Testing

-- PostgreSQL to SparkSQL conversion:

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


PySpark jobs for these queries:

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct, max, sum, avg, year, current_date

def process_actor_data(spark, actor_films_df):
    actor_stats = actor_films_df.groupBy("actorid").agg(
        collect_list(
            struct(
                "film",
                "year", 
                "votes",
                "rating", 
                "filmid"
            )
        ).alias("films"),
        max("year").alias("most_recent_year"),
        # did they acted this year?
        (sum("year") == year(current_date())).alias("is_active") 
    )

    final_data = actor_films_df.join(actor_stats,on="actorid")

    ##looking only at their most recent year
    final_data = final_data.filter(final_data.year == final_data.most_recent_year)
    final_data = final_data.groupBy(
        "actorid",
        "films", 
        "most_recent_year",
        "is_active"
    ).agg(avg("rating").alias("avg_rating"))

    final_data = final_data.withColumn(
        "quality_class",
        when(final_data.avg_rating > 8, "star")
        .when(final_data.avg_rating > 7, "good") 
        .when(final_data.avg_rating > 6, "avg")
        .otherwise("bad"))

    final_data = final_data.drop("avg_rating")

    return final_data

def main():
    spark = SparkSession.builder.appName("process_data").getOrCreate()
    actor_films = spark.read.table("actor_films")
    result = process_data(spark, actor_films)
    result.write.format("delta").mode("overwrite").saveAsTable("actors")

if __name__ == "__main__":
    main()


A simple test for actor data processing:

from pyspark.sql import SparkSession
import pytest

def simple_actor_processing(spark):
    # Let's say we have 2 actors:One with high ratings and another with lower ratings
    test_data = [("actor1", "blockbuster", 2023, 1000, 9.0, "film1"),("actor2", "okayish_movie", 2023, 500, 6.5, "film2")]
    columns = ["actorid", "film", "year", "votes", "rating", "filmid"]
    test_df = spark.createDataFrame(test_data, columns)

    result = process_actor_data(spark, test_df)
    results_list = result.collect()
    
    # some simple checks:
    assert len(results_list) == 2

    actor1 = result.filter(result.actorid == "actor1").first()
    assert actor1.quality_class == "star"

    actor2 = result.filter(result.actorid == "actor2").first()
    assert actor2.quality_class == "avg"

    assert all(row.is_active for row in results_list)
