# Week 4 Applying Analytical Patterns
The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`

-- CREATE TYPE season_stats AS (
-- 				season INTEGER,
-- 				gp INTEGER,
-- 				pts REAL,
-- 				reb REAL,
-- 				ast REAL
-- )

-- CREATE TYPE scoring_class AS ENUM('star','good','avg','bad')

-- DROP TABLE players

CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	current_season INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY(player_name,current_season)
)


INSERT INTO players
WITH years AS (
		SELECT *
		FROM generate_series(1996,2022) AS season
	),
	-- player_seasons_reset AS (
	-- 	SELECT *
	-- 	FROM player_seasons /*manually reset the primary key index to negate the duplication error*/
	-- ),
	p AS (
		SELECT player_name, MIN(season) AS first_season
		FROM player_seasons
		GROUP BY player_name
	),
	players_and_seasons AS (
		SELECT p.player_name,
				p.first_season,
				ps.height,
				ps.college,
				ps.country,
				ps.draft_year,
				ps.draft_round,
				ps.draft_number,
				ps.gp,
				ps.pts,
				ps.reb,
				ps.ast,
				ps.season
		FROM p 
		JOIN player_seasons ps
		ON p.first_season <= ps.season
	),
	windowed AS (
		SELECT ps.player_name,
				ps.season,
				array_remove(ARRAY_AGG(CASE WHEN p1.season IS NOT NULL THEN 
											CAST(ROW(p1.season,p1.gp,p1.pts,p1.reb,p1.ast) as season_stats)
										END)
											OVER (PARTITION BY ps.player_name ORDER BY COALESCE(p1.season,ps.season)),NULL)
											as seasons

		FROM players_and_seasons ps
		LEFT JOIN player_seasons p1
		ON ps.player_name=p1.player_name AND ps.season=p1.season
		ORDER BY 1,2
		-- ORDER BY ps.player_name,ps.season
	),
	static AS (
		SELECT player_name,
				max(height) as height,
				max(college) as college,
				max(country) as country,
				max(draft_year) as draft_year,
				max(draft_round) as draft_round,
				max(draft_number) as draft_number
		FROM player_seasons
		GROUP BY player_name
	)
	
SELECT w.player_name,
		s.height,
		s.college,
		s.country,
		s.draft_year,
		s.draft_round,
		s.draft_number,
		seasons as season_stats,
		CASE WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
			WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
			WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'avg'
			ELSE 'bad'
		END::scoring_class as scoring_class,
		w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_season,
		w.season as current_season,
		(seasons[CARDINALITY(seasons)]::season_stats).season = season as is_active

	FROM windowed w
	JOIN static s ON w.player_name = s.player_name


/*TEST CASE to negate any duplication - works good*/
select * from player_seasons
where player_name = 'Aaron Brooks'

TRUNCATE TABLE players 

INSERT INTO players
WITH years AS (
		SELECT *
		FROM generate_series(1996,2022) AS season
	),
	player_seasons0 as (
		select * from player_seasons
		where player_name in ('Aaron Brooks','Michael Jordan') 
	),
	p AS (
		SELECT player_name, MIN(season) AS first_season
		FROM player_seasons0
		GROUP BY player_name
	),
	players_and_seasons AS (
		SELECT p.player_name,
				p.first_season,
				ps.height,
				ps.college,
				ps.country,
				ps.draft_year,
				ps.draft_round,
				ps.draft_number,
				ps.gp,
				ps.pts,
				ps.reb,
				ps.ast,
				ps.season
		FROM p 
		JOIN player_seasons0 ps
		ON p.first_season <= ps.season
	),
	windowed AS (
		SELECT ps.player_name,
				ps.season,
				array_remove(ARRAY_AGG(CASE WHEN p1.season IS NOT NULL THEN 
											CAST(ROW(p1.season,p1.gp,p1.pts,p1.reb,p1.ast) as season_stats)
										END)
											OVER (PARTITION BY ps.player_name ORDER BY COALESCE(p1.season,ps.season)),NULL)
											as seasons

		FROM players_and_seasons ps
		LEFT JOIN player_seasons0 p1
		ON ps.player_name=p1.player_name AND ps.season=p1.season
		ORDER BY 1,2
		-- ORDER BY ps.player_name,ps.season
	),
	static AS (
		SELECT player_name,
				max(height) as height,
				max(college) as college,
				max(country) as country,
				max(draft_year) as draft_year,
				max(draft_round) as draft_round,
				max(draft_number) as draft_number
		FROM player_seasons0
		GROUP BY player_name
	)
	
SELECT w.player_name,
		s.height,
		s.college,
		s.country,
		s.draft_year,
		s.draft_round,
		s.draft_number,
		seasons as season_stats,
		CASE WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
			WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
			WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'avg'
			ELSE 'bad'
		END::scoring_class as scoring_class,
		w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_season,
		w.season as current_season,
		(seasons[CARDINALITY(seasons)]::season_stats).season = season as is_active

	FROM windowed w
	JOIN static s ON w.player_name = s.player_name

	select * from players
	select * from player_seasons

 -----

WITH player_states AS (
  SELECT 
    p1.player_name,
    p1.current_season,
    p1.is_active as current_active,
    LAG(p1.is_active) OVER (PARTITION BY p1.player_name ORDER BY p1.current_season) as prev_active,
    CASE
      WHEN LAG(p1.is_active) OVER (PARTITION BY p1.player_name ORDER BY p1.current_season) IS NULL 
        AND p1.is_active = true THEN 'New'
      WHEN p1.is_active = false AND LAG(p1.is_active) OVER (PARTITION BY p1.player_name ORDER BY p1.current_season) = true 
        THEN 'Retired'
      WHEN p1.is_active = true AND LAG(p1.is_active) OVER (PARTITION BY p1.player_name ORDER BY p1.current_season) = true
        THEN 'Continued Playing'
      WHEN p1.is_active = true AND LAG(p1.is_active) OVER (PARTITION BY p1.player_name ORDER BY p1.current_season) = false
        THEN 'Returned from Retirement'
      WHEN p1.is_active = false AND LAG(p1.is_active) OVER (PARTITION BY p1.player_name ORDER BY p1.current_season) = false
        THEN 'Stayed Retired'
    END as player_state
  FROM players p1
)
SELECT 
  player_name,
  current_season,
  player_state
FROM player_states
ORDER BY player_name, current_season
    
  
- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
  - Aggregate this dataset along the following dimensions
    - player and team
      - Answer questions like who scored the most points playing for one team?
    - player and season
      - Answer questions like who scored the most points in one season?
    - team
      - Answer questions like which team has won the most games?

WITH game_details_agg AS (
  SELECT 
    gd.player_name,
    gd.team_abbreviation,
    g.season,
    SUM(gd.pts) as total_points,
    COUNT(DISTINCT gd.game_id) as games_played,
    SUM(CASE 
      WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1) OR
           (gd.team_id = g.visitor_team_id AND g.home_team_wins = 0) 
      THEN 1 
      ELSE 0 
    END) as wins,
    GROUPING(gd.player_name, gd.team_abbreviation, g.season) as grouping_id
  FROM game_details gd
  JOIN games g ON gd.game_id = g.game_id
  GROUP BY 
    GROUPING SETS (
      (gd.player_name, gd.team_abbreviation),
      (gd.player_name, g.season),
      (gd.team_abbreviation)
    )
)
SELECT
  player_name,
  team_abbreviation, 
  season,
  total_points,
  games_played,
  wins,
  ROUND(CAST(wins AS DECIMAL) / NULLIF(games_played, 0) * 100, 1) as win_pct,
  CASE grouping_id
    WHEN 0 THEN 'Player & Team' 
    WHEN 1 THEN 'Player & Season'
    WHEN 3 THEN 'Team Only'
  END as grouping_type
FROM game_details_agg
WHERE 
  -- Filter for meaningful aggregations
  (games_played >= 20 OR grouping_id = 3)
ORDER BY
  CASE grouping_id
    WHEN 0 THEN total_points 
    WHEN 1 THEN total_points
    WHEN 3 THEN wins
  END DESC
      
- A query that uses window functions on `game_details` to find out the following things:
  - What is the most games a team has won in a 90 game stretch? 
  - How many games in a row did LeBron James score over 10 points a game?

-- Most wins in a 90 game stretch by team
WITH team_games AS (
  SELECT 
    g.game_date,
    gd.team_abbreviation,
    CASE 
      WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1) OR
           (gd.team_id = g.visitor_team_id AND g.home_team_wins = 0) 
      THEN 1 
      ELSE 0 
    END as won_game
  FROM game_details gd
  JOIN games g ON gd.game_id = g.game_id
  GROUP BY g.game_date, gd.team_abbreviation, gd.team_id, g.home_team_id, g.visitor_team_id, g.home_team_wins
),
rolling_wins AS (
  SELECT
    team_abbreviation,
    game_date,
    SUM(won_game) OVER (
      PARTITION BY team_abbreviation 
      ORDER BY game_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as wins_in_90,
    COUNT(1) OVER (
      PARTITION BY team_abbreviation 
      ORDER BY game_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as games_in_window
  FROM team_games
)
SELECT
  team_abbreviation,
  game_date,
  wins_in_90
FROM rolling_wins
WHERE games_in_window = 90
ORDER BY wins_in_90 DESC
LIMIT 5

-- LeBron James scoring streak over 10 points
WITH lebron_games AS (
  SELECT 
    g.game_date,
    gd.pts,
    CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END as scored_over_10,
    SUM(CASE WHEN gd.pts <= 10 THEN 1 ELSE 0 END) OVER (ORDER BY g.game_date) as streak_group
  FROM game_details gd
  JOIN games g ON gd.game_id = g.game_id
  WHERE gd.player_name = 'LeBron James'
  ORDER BY g.game_date
)
SELECT 
  MIN(game_date) as streak_start,
  MAX(game_date) as streak_end,
  COUNT(*) as streak_length
FROM lebron_games
GROUP BY streak_group
HAVING MIN(scored_over_10) = 1 
ORDER BY COUNT(*) DESC
LIMIT 1



Please add these queries into a folder `homework/<discord-username>`