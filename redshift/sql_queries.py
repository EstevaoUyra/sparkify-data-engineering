import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER );
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INTEGER NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    artist_latitude FLOAT, 
    artist_longitude FLOAT, 
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR, 
    duration FLOAT, 
    year INTEGER );
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP,
    user_id VARCHAR NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id VARCHAR NOT NULL,
    location VARCHAR,
    user_agent VARCHAR );
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR NOT NULL);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR, 
    year INT, 
    duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE artists (
   artist_id VARCHAR PRIMARY KEY, 
   name VARCHAR NOT NULL, 
   location VARCHAR, 
   latitude FLOAT, 
   longitude FLOAT);
""")

time_table_create = ("""
CREATE TABLE time (
   start_time TIMESTAMP PRIMARY KEY,
   hour INT, 
   day INT, 
   week INT, 
   month INT, 
   year INT, 
   weekday INT);
""")


# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json'
    compupdate off 
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off 
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  
       dateadd(millisecond, e.ts, '1970-01-01 00:00:00')         AS start_time,
       e.userId      AS user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId   AS session_id,
       e.location,
       e.userAgent   AS user_agent
        
FROM staging_events e
JOIN staging_songs s   
ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
""")


user_table_insert = ("""
INSERT INTO users 
SELECT DISTINCT
    userId          AS user_id,
    firstName       AS first_name,
    lastName        AS last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs SELECT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")


artist_table_insert = ("""
INSERT INTO artists
SELECT
    artist_id,
    artist_name          AS name,
    artist_location      AS location,
    artist_latitude      AS latitude,
    artist_longitude     AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
    start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day from start_time) AS day,
    EXTRACT(week from start_time) AS week,
    EXTRACT(month from start_time) AS month,
    EXTRACT(year from start_time) AS year,
    EXTRACT(dayofweek from start_time) AS weekday
FROM songplays
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
