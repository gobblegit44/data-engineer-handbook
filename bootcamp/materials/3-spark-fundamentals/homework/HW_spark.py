  from pyspark.sql import SparkSession
  from pyspark.sql.functions import broadcast

  # Create Spark session
  spark = SparkSession.builder \
      .appName("Jupyter") \
      .getOrCreate()

  # Disable automatic broadcast join
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  # Read data
  medals_df = spark.read.csv("medals")
  maps_df = spark.read.csv("maps")

  # Broadcast the smaller tables
  medals_broadcast = broadcast(medals_df)
  maps_broadcast = broadcast(maps_df)

  # Read the larger tables that we'll bucket
  match_details_df = spark.read.csv("match_details")
  matches_df = spark.read.csv("matches") 
  medal_matches_players_df = spark.read.csv("medal_matches_players")

  # Create bucketed tables
  match_details_df.write \
      .bucketBy(16, "match_id") \
      .mode("overwrite") \
      .saveAsTable("match_details_bucketed")

  matches_df.write \
      .bucketBy(16, "match_id") \
      .mode("overwrite") \
      .saveAsTable("matches_bucketed")

  medal_matches_players_df.write \
      .bucketBy(16, "match_id") \
      .mode("overwrite") \
      .saveAsTable("medal_matches_players_bucketed")

  # Read the bucketed tables
  match_details_bucketed = spark.table("match_details_bucketed")
  matches_bucketed = spark.table("matches_bucketed")
  medal_matches_players_bucketed = spark.table("medal_matches_players_bucketed")

  # Join the bucketed tables
  joined_df = match_details_bucketed \
      .join(matches_bucketed, "match_id") \
      .join(medal_matches_players_bucketed, "match_id")

  # Calculate average kills per game by player
  avg_kills_by_player = match_details_bucketed \
      .groupBy("player_id") \
      .agg({"kills": "avg"}) \
      .orderBy("avg(kills)", ascending=False)

  print("Players with highest average kills per game:")
  avg_kills_by_player.show(5)

  # Find most played playlist
  most_played_playlist = matches_bucketed \
      .groupBy("playlist") \
      .count() \
      .orderBy("count", ascending=False)

  print("Most played playlists:")
  most_played_playlist.show(5)

  # Find most played map
  most_played_map = matches_bucketed \
      .join(maps_broadcast, "map_id") \
      .groupBy("map_name") \
      .count() \
      .orderBy("count", ascending=False)

  print("Most played maps:")
  most_played_map.show(5)

  # Find maps with most Killing Spree medals
  killing_spree_by_map = medal_matches_players_bucketed \
      .join(medals_broadcast, "medal_id") \
      .join(matches_bucketed, "match_id") \
      .join(maps_broadcast, "map_id") \
      .where("medal_name = 'Killing Spree'") \
      .groupBy("map_name") \
      .count() \
      .orderBy("count", ascending=False)

  print("Maps with most Killing Spree medals:")
  killing_spree_by_map.show(5)

  # Try different sortWithinPartitions approaches and compare sizes
  
  # Sort by playlist (low cardinality)
  playlist_sorted = most_played_playlist \
      .repartition(4) \
      .sortWithinPartitions("playlist") \
      .cache()
      
  print("\nSize after sorting by playlist:")
  print(playlist_sorted._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())

  # Sort by map (low cardinality) 
  map_sorted = most_played_map \
      .repartition(4) \
      .sortWithinPartitions("map_name") \
      .cache()
      
  print("\nSize after sorting by map:")
  print(map_sorted._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())

  # Sort by count (high cardinality)
  count_sorted = most_played_map \
      .repartition(4) \
      .sortWithinPartitions("count") \
      .cache()
      
  print("\nSize after sorting by count:")
  print(count_sorted._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())

  
  
  
  