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
    


