-- CREATE TYPE season_stats AS (
-- 				season INTEGER,
-- 				gp INTEGER,
-- 				pts REAL,
-- 				reb REAL,
-- 				ast REAL
-- )

-- CREATE TYPE scoring_class AS ENUM('star','good','avg','bad')

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
	player_seasons_reset AS (
		SELECT *
		FROM player_seasons /*manually reset the primary key index to negate the duplication error*/
	),
	p AS (
		SELECT player_name, MIN(season) AS first_season
		FROM player_seasons_reset
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
		JOIN player_seasons_reset ps
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
		LEFT JOIN player_seasons_reset p1
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
		FROM player_seasons_reset
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