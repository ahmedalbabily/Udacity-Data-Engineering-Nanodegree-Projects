import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSON_PATH")
SONG = config.get("S3", "SONG_DATA")
REGION = "us-west-2"
ARN = config.get("IAM_ROLE", "ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events
(
    artist VARCHAR(255),
    auth VARCHAR(255),
    first_name VARCHAR(255),
    gender CHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR(255),
    length NUMERIC(20,10),
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration NUMERIC(20,10),
    session_id INTEGER,
    song VARCHAR(255),
    status INTEGER,
    ts BIGINT,
    user_agent VARCHAR(255),
    user_id INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR(255),
    artist_latitude NUMERIC(20,10),
    artist_longitude NUMERIC(20,10),
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(255),
    title VARCHAR(255),
    duration NUMERIC(20,10),
    year INTEGER
)

""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(
songplay_id int IDENTITY(0,1) PRIMARY KEY,  
start_time bigint REFERENCES time (start_time) NOT NULL, 
user_id int REFERENCES users (user_id) NOT NULL ,
level varchar, song_id varchar REFERENCES songs (song_id),
artist_id varchar REFERENCES artists (artist_id),
session_id int,
location varchar, 
user_agent varchar)
""")



user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(
user_id int PRIMARY KEY, 
first_name varchar, 
last_name varchar,
gender varchar,
level varchar)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
(
song_id varchar PRIMARY KEY,
title varchar NOT NULL,
artist_id varchar REFERENCES artists (artist_id), 
year int,
duration decimal NOT NULL)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(
artist_id varchar PRIMARY KEY, 
name varchar NOT NULL,
location varchar, 
latitude double precision,
longitude double precision)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
(start_time bigint PRIMARY KEY,
hour int, day int,
week int, month int,
year int, weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
        FROM {} 
        iam_role {} 
        region 'us-west-2'
        FORMAT AS JSON {} 
        timeformat 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG, ARN,LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs 
        FROM {}
        iam_role {}
        region 'us-west-2'
        FORMAT AS JSON 'auto' 
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG, ARN)
# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        SELECT DISTINCT e.ts, 
                        e.userId, 
                        e.level, 
                        s.song_id, 
                        s.artist_id, 
                        e.sessionId, 
                        e.location, 
                        e.userAgent
        FROM staging_events e 
        INNER JOIN staging_songs s 
            ON e.song = s.title AND e.artist = s.artist_name
        WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT e.userId, 
                        e.firstName, 
                        e.lastName, 
                        e.gender, 
                        e.level
        FROM staging_events e
        WHERE e.userId IS NOT NULL;

""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration) VALUES (%s,%s,%s,%s,%s) on conflict do nothing
  INSERT INTO songs (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT s.song_id, 
                        s.title, 
                        s.artist_id, 
                        s.year, 
                        s.duration
        FROM staging_songs s
        WHERE ss.song_id IS NOT NULL;


""")

artist_table_insert = ("""
  INSERT INTO artists (artist_id, name, location, latitude, logitude)
        SELECT DISTINCT s.artist_id, 
                        s.artist_name, 
                        s.artist_location,
                        s.artist_latitude,
                        s.artist_longitude
        FROM staging_songs s
        WHERE s.artist_id IS NOT NULL;

""")


time_table_insert = ("""
 INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT  se.ts,
                        EXTRACT(hour from e.ts),
                        EXTRACT(day from e.ts),
                        EXTRACT(week from e.ts),
                        EXTRACT(month from e.ts),
                        EXTRACT(year from e.ts),
                        EXTRACT(weekday from e.ts)
        FROM staging_events e
        WHERE e.page = 'NextSong';

""")


# QUERY LISTS

create_table_queries = [time_table_create,artist_table_create,song_table_create,user_table_create,songplay_table_create,staging_songs_table_create,staging_events_table_create]

drop_table_queries = [songplay_table_drop,time_table_drop,user_table_drop,song_table_drop,artist_table_drop,staging_events_table_drop,staging_songs_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
