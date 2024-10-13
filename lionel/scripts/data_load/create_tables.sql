
-- Write SQL code for SQLite databases
CREATE TABLE teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE team_seasons (
    web_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    PRIMARY KEY (web_id, season),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    full_name TEXT NOT NULL
);

CREATE TABLE player_seasons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    web_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    position TEXT NOT NULL,
    FOREIGN KEY (player_id) REFERENCES players(id)
);

CREATE TABLE fixtures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    home_id INTEGER NOT NULL,
    away_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    gameweek INTEGER NOT NULL,
    gameweek_id INTEGER NOT NULL,
    kickoff_time TEXT NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    fixture_season_id INTEGER NOT NULL,
    FOREIGN KEY (home_id) REFERENCES teams(id),
    FOREIGN KEY (away_id) REFERENCES teams(id),
    FOREIGN KEY (gameweek_id) REFERENCES gameweeks(id)
);

CREATE TABLE gameweeks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER NOT NULL,
    gameweek INTEGER NOT NULL,
    deadline DATETIME
);

-- -- Not implemented - but potentially useful with fixture_ids
-- CREATE TABLE players_teams (
--     player_id INTEGER NOT NULL,
--     team_id INTEGER NOT NULL,
--     fixture_id INTEGER NOT NULL,
--     PRIMARY KEY (player_id, team_id),
--     FOREIGN KEY (player_id) REFERENCES players(id),
--     FOREIGN KEY (team_id) REFERENCES teams(id),
--     FOREIGN KEY (fixture_id) REFERENCES fixtures(id)
-- );


CREATE TABLE stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    -- team_id INTEGER NOT NULL,
    fixture_id INTEGER NOT NULL,
    gameweek_id INTEGER NOT NULL,
    -- position TEXT NOT NULL, # in player_seasos
    is_home BOOLEAN NOT NULL,
    season INTEGER NOT NULL,

    -- Then the actual stats
    total_points INTEGER NOT NULL,
    minutes INTEGER NOT NULL,
    goals_scored INTEGER NOT NULL,
    assists INTEGER NOT NULL,
    clean_sheets INTEGER ,
    goals_conceded INTEGER ,
    own_goals INTEGER ,
    penalties_saved INTEGER ,
    penalties_missed INTEGER ,
    yellow_cards INTEGER ,
    red_cards INTEGER ,
    saves INTEGER ,
    bonus INTEGER ,
    bps INTEGER ,
    influence REAL ,
    creativity REAL ,
    threat REAL ,
    ict_index REAL ,
    expected_goals REAL ,
    expected_assists REAL ,
    expected_goal_involvements REAL ,
    -- expected_clean_sheets REAL ,
    value REAL ,
    transfers_balance INTEGER ,
    selected INTEGER ,
    transfers_in INTEGER ,
    transfers_out INTEGER,

    FOREIGN KEY (player_id) REFERENCES players(id),
    -- FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (fixture_id) REFERENCES fixtures(id),
    FOREIGN KEY (gameweek_id) REFERENCES gameweeks(id)
);

CREATE VIEW training AS
SELECT 
    CONCAT(p.id, "_", p.name) AS player,
    ps.position,
    ht.name as home_team, at.name as away_team,
    f.home_score AS home_goals, f.away_score AS away_goals, f.gameweek, f.season,
    s.minutes, s.total_points AS points, s.goals_scored, s.assists, s.is_home

FROM stats AS s
INNER JOIN fixtures AS f
ON s.fixture_id = f.id

INNER JOIN player_seasons as ps
ON s.player_id = ps.id

INNER JOIN players as p
ON ps.player_id = p.id

INNER JOIN teams as ht
ON f.home_id = ht.id

INNER JOIN teams as at
ON f.away_id = at.id;

