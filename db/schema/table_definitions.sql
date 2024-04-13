CREATE TABLE IF NOT EXISTS PowerplayStats (
    match_id INT, --unique match id
    batting_team VARCHAR(255), -- team that batted
    pp_runs_scored DECIMAL(5,1), -- team that bowled
    pp_wickets INT, -- #wickets in powerplay
    pp_fours INT, -- #fours in powerplay
    pp_sixes INT, -- #sixes in powerplay
    pp_dots INT, -- #dots in powerplay
    PRIMARY KEY (match_id, batting_team),-- primary key
    FOREIGN KEY (match_id) REFERENCES Matches(match_id)
);


CREATE TABLE IF NOT EXISTS MatchStats (
    match_id INT,
    batting_team VARCHAR(255),
    bowling_team VARCHAR(255),
    ms_runs_scored DECIMAL(5,1),
    ms_wickets INT,
    ms_fours INT,
    ms_sixes INT,
    ms_dots INT,
    ms_no_balls DECIMAL(3,1),
    ms_wides DECIMAL(3,1),
    PRIMARY KEY (match_id, batting_team, bowling_team)
);

CREATE TABLE IF NOT EXISTS MatchDetails (
    match_id INT PRIMARY KEY,
    season INT,
    team1 VARCHAR(255),
    team2 VARCHAR(255),
    date DATE,
    match_number INT,
    venue VARCHAR(255),
    city VARCHAR(255),
    toss_winner VARCHAR(255),
    toss_decision VARCHAR(50),
    player_of_match VARCHAR(255),
    umpire1 VARCHAR(255),
    umpire2 VARCHAR(255),
    reserve_umpire VARCHAR(255),
    match_referee VARCHAR(255),
    winner VARCHAR(255),
    winner_runs INT,
    winner_wickets INT,
    match_type VARCHAR(100),
    home_team VARCHAR(255),
    month INT,
    day INT,
    day_name VARCHAR(100)
);

