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