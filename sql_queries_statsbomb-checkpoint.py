import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES


staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_matches_table_drop = "DROP TABLE IF EXISTS staging_matches"


matches_fact_table_drop = "DROP TABLE IF EXISTS matches_fact CASCADE"

match_table_drop = "DROP TABLE IF EXISTS match "
players_table_drop = "DROP TABLE IF EXISTS player "
teams_table_drop = "DROP TABLE IF EXISTS team"

# CREATE TABLES
'''
staging_events_table_create= (
    """ 
    CREATE TABLE IF NOT EXISTS staging_events(
		bad_behaviour ,
		ball_receipt ,
		ball_recovery ,
		block ,
		carry ,
		clearance ,
		counterpress ,
		dribble ,
		duel ,
		duration ,
		foul_committed ,
		foul_won ,
		goalkeeper ,
		id ,
		index ,
		texterception ,
		location ,
		minute ,
		miscontrol ,
		off_camera ,
		out ,
		pass ,
		period ,
		play_pattern ,
		player ,
		position ,
		possession ,
		possession_team ,
		related_events ,
		second ,
		shot ,
		substitution ,
		tactics ,
		team ,
		timestamp ,
		type ,
		under_pressure 
        ) /*DISTKEY ()*/;
""")

staging_matches_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_matches  (
		away_score ,
		away_team ,
		competition ,
		competition_stage ,
		home_score ,
		home_team ,
		kick_off ,
		last_updated ,
		last_updated_360 ,
		match_date ,
		match_id ,
		match_status ,
		match_status_360 ,
		match_week ,
		metadata ,
		referee ,
		season ,
		stadium 
  )
  """)
'''

match_table_create = (
    """
CREATE TABLE IF NOT EXISTS match  (
    competition_id text  ,
match_id text not NULL PRIMARY KEY,
home_team_id text ,
away_team_id text ,
away_score text ,
home_score text,
stadium_id text ,
stadium_name text ,
stadium_country_name text ,
time int
    )DISTKEY (match_id)
""")

player_table_create = (
    """
CREATE TABLE IF NOT EXISTS player  (
    team_id text ,
    player_id  text NOT NULL PRIMARY KEY,
    player_name  text,
    position_id text  ,
    position_name text   
)DISTKEY (player_id)
""")

team_table_create = (
    """
CREATE TABLE IF NOT EXISTS team  (
    team_id text NOT NULL PRIMARY KEY,
    team_name text  ,
    country_id text
)DISTKEY (team_id)
""")

#  TABLES
ARN = config.get("IAM_ROLE", "ARN")
#MATCHES_DATA = config.get("S3", "MATCH_DATA")
#EVENTS_DATA = config.get("S3", "EVENTS_DATA")

MATCH_TABLE = config.get("S3", "MATCH_TABLE")
PLAYER_TABLE = config.get("S3", "PLAYER_TABLE")
TEAM_TABLE = config.get("S3", "TEAM_TABLE")

'''
staging_events_copy = (
""" copy staging_events from {}
    iam_role  '{}'
    JSON 'auto'
    region 'us-west-2';
""").format(EVENTS_DATA , ARN)
staging_matches_copy = (
""" copy staging_matches from {}
    iam_role  '{}'
    JSON 'auto'
    region 'us-west-2';
""").format(MATCHES_DATA, ARN )
'''


match_copy = (
""" copy match from {}
    iam_role  '{}'
    csv
    delimiter ','
    FILLRECORD
    region 'us-west-2';
""").format(MATCH_TABLE, ARN )

player_copy = (
""" 
copy player from {}
iam_role  '{}'
csv
FILLRECORD 
IGNOREHEADER 1;
""").format(PLAYER_TABLE, ARN )

team_copy = (
""" copy team from {}
    iam_role  '{}'
    csv
    FILLRECORD 
    IGNOREHEADER 1;
""").format(TEAM_TABLE, ARN )



# QUERY LISTS

create_table_queries = [ match_table_create, player_table_create, team_table_create]

drop_table_queries = [staging_events_table_drop,staging_matches_table_drop,matches_fact_table_drop, match_table_drop, players_table_drop, teams_table_drop]

copy_table_queries = [match_copy,player_copy, team_copy]

#insert_FACT_table_query = []
