-- Table for xGoals per game 
CREATE TABLE xgoals_games (
    match_id SERIAL PRIMARY KEY,
    date DATE,
    time VARCHAR(50),
    home_team VARCHAR(50),
    home_goals INT,
    home_xG FLOAT,
    home_xGp FLOAT,
    away_team VARCHAR(50),
    away_goals INT,
    away_xG FLOAT,
    away_xGp FLOAT,
    goal_difference INT,
    xG_difference FLOAT,
    xGp_difference FLOAT,
    final_result INT,
    home_xPts FLOAT,
    away_xPts FLOAT
);
-- table for Salaries per game 
CREATE TABLE salaries (
    id SERIAL PRIMARY KEY,
    team VARCHAR(50),
    -- Name of the team
    num_players INT,
    -- Number of players (N)
    total_guaranteed FLOAT,
    -- Total guaranteed salary (TotalGuar)
    avg_guaranteed FLOAT,
    -- Average guaranteed salary (AvgGuar)
    median_guaranteed FLOAT,
    -- Median guaranteed salary (MedGuar)
    stddev_guaranteed FLOAT -- Standard deviation of guaranteed salary (StdDevGuar)
);