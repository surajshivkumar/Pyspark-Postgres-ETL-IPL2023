import psycopg2
from psycopg2 import sql

class DatabaseManager:
    def __init__(self, config):
        """Initialize the database connection and create tables."""
        self.conn = psycopg2.connect(
            dbname=config['dbname'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )
        self.conn.autocommit = True
        self.create_table_match_info()
        self.create_table_match_stats()
        self.create_table_powerplay_stats()

    def create_table_match_info(self):
        command = \
            """
            CREATE TABLE IF NOT EXISTS MatchDetails (
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
                    match_id INT PRIMARY KEY,
                    home_team VARCHAR(255),
                    month VARCHAR(20),
                    day INT,
                    day_name VARCHAR(100)
                );
            """
        with self.conn.cursor() as cursor:
                cursor.execute(command)


    def create_table_match_stats(self):
         
         command = \
         '''
            CREATE TABLE IF NOT EXISTS MatchStats 
        (
            match_id INT,
            ms_batting_team VARCHAR(255),
            ms_bowling_team VARCHAR(255),
            ms_runs_scored DECIMAL(5,1),
            ms_wickets INT,
            ms_fours INT,
            ms_sixes INT,
            ms_dots INT,
            ms_no_balls DECIMAL(3,1),
            ms_wides DECIMAL(3,1),
            PRIMARY KEY (match_id, ms_batting_team, ms_bowling_team)
        );
         '''
         with self.conn.cursor() as cursor:
            cursor.execute(command)
    
    def create_table_powerplay_stats(self):
         
         command = \
         '''
            CREATE TABLE IF NOT EXISTS PowerplayStats (
            match_id INT, --unique match id
            pp_batting_team VARCHAR(255), -- team that batted
            pp_bowling_team VARCHAR(255), -- team that batted
            pp_runs_scored DECIMAL(5,1), -- team that bowled
            pp_wickets INT, -- #wickets in powerplay
            pp_fours INT, -- #fours in powerplay
            pp_sixes INT, -- #sixes in powerplay
            pp_dots INT, -- #dots in powerplay
            pp_no_balls INT, -- #dots in powerplay
            pp_wides INT, -- #dots in powerplay
            PRIMARY KEY (match_id, pp_batting_team)
        );
         '''
         with self.conn.cursor() as cursor:
            cursor.execute(command)
         

    def insert_data(self, table, data):
        """Insert data into the table."""
        # Assuming data is a list of tuples corresponding to the table columns
        with self.conn.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(data[0]))  # create placeholders
            query = sql.SQL('INSERT INTO {} VALUES ({})').format(
                sql.Identifier(table),
                sql.SQL(placeholders)
            )
            cursor.executemany(query, data)
            self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.close()



