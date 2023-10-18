import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA       = config.get('S3','LOG_DATA')
LOG_JSONPATH   = config.get('S3','LOG_JSONPATH')
SONG_DATA      = config.get('S3','SONG_DATA')
DWH_ROLE_ARN   = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_table 
(
    artist          VARCHAR(300) , 
    auth            VARCHAR(20) ,
    firstName       VARCHAR(100) ,
    gender          CHAR(1) ,
    itemInSession   INTEGER ,
    lastName        VARCHAR(100) ,
    length          NUMERIC ,
    level           VARCHAR(100) ,
    location        VARCHAR(100) ,
    method          VARCHAR(5) ,
    page            VARCHAR(100) ,
    registration    NUMERIC ,
    sessionId       INTEGER ,
    song            VARCHAR(200) ,
    status          INTEGER ,
    ts              NUMERIC ,
    userAgent       VARCHAR(400) ,
    userId          NUMERIC 
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_table 
(
    num_songs           NUMERIC , 
    artist_id           VARCHAR(100) ,
    artist_latitude     NUMERIC, 
    artist_longitude    NUMERIC, 
    artist_location     VARCHAR(400), 
    artist_name         VARCHAR(400) , 
    song_id             VARCHAR(100) , 
    title               VARCHAR(400) , 
    duration            NUMERIC, 
    year                INTEGER
)   
""")

songplay_table_create = ("""
CREATE TABLE songplay_table 
(
    f_songplay_id         BIGINT IDENTITY(1, 1), 
    f_start_time          TIMESTAMP, 
    f_user_id             NUMERIC, 
    f_level               VARCHAR(100), 
    f_song_id             VARCHAR(100), 
    f_artist_id           VARCHAR(100), 
    f_session_id          INTEGER, 
    f_location            VARCHAR(100), 
    f_user_agent          VARCHAR(400)
)
""")

user_table_create = ("""
CREATE TABLE user_table 
(
    d_user_id             NUMERIC PRIMARY KEY , 
    d_first_name          VARCHAR(50) ,         
    d_last_name           VARCHAR(50), 
    d_gender              CHAR(1) , 
    d_level               VARCHAR(100)
)
""")

song_table_create = ("""
CREATE TABLE song_table
(
    d_song_id             VARCHAR(100) PRIMARY KEY, 
    d_title               VARCHAR(400) , 
    d_artist_id           VARCHAR(100) , 
    d_year                INTEGER, 
    d_duration            NUMERIC
)
""")

artist_table_create = ("""
CREATE TABLE artist_table
(
    d_artist_id           VARCHAR(100) PRIMARY KEY, 
    d_name                VARCHAR(400), 
    d_location            VARCHAR(400), 
    d_lattitude           NUMERIC, 
    d_longitude           NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE time_table
(
    d_start_time        TIMESTAMP NOT NULL, 
    d_hour              INTEGER NOT NULL, 
    d_day               INTEGER NOT NULL, 
    d_week              INTEGER NOT NULL, 
    d_month             INTEGER NOT NULL, 
    d_year              INTEGER NOT NULL, 
    d_weekday           BOOLEAN NOT NULL

)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table from {} 
iam_role {}
FORMAT AS JSON {}
compupdate off
region 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs_table from {} 
iam_role {}
FORMAT AS JSON 'auto'
compupdate off 
region 'us-west-2';
""").format(SONG_DATA, DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (f_start_time, f_user_id, f_level, f_song_id, f_artist_id, f_session_id, f_location, f_user_agent)
SELECT  
    (timestamp 'epoch' + e_table.ts::numeric * interval '0.001 seconds')  AS f_start_time,
    e_table.userId AS f_user_id,
    e_table.level AS f_level,
    s_table.song_id AS f_song_id,
    s_table.artist_id AS f_artist_id,
    e_table.sessionId AS f_session_id,
    e_table.location AS f_location,
    e_table.userAgent AS f_user_agent
FROM staging_events_table e_table
LEFT JOIN staging_songs_table s_table 
ON (s_table.title = e_table.song
AND s_table.artist_name = e_table.artist)
WHERE e_table.song IS NOT NULL AND e_table.userId IS NOT NULL;
""")


user_table_insert = ("""
INSERT INTO user_table (d_user_id, d_first_name , d_last_name, d_gender,d_level)
SELECT DISTINCT 
        userId AS d_user_id,
        firstName AS d_first_name,
        lastName AS d_last_name,
        gender AS d_gender,
        level AS d_level
FROM staging_events_table
WHERE userId IS NOT NULL;

""")

song_table_insert = ("""
INSERT INTO song_table (d_song_id, d_title, d_artist_id, d_year, d_duration)
SELECT DISTINCT
        song_id AS d_song_id,
        title AS d_title,
        artist_id AS d_artist_id,
        year AS d_year,
        duration AS d_duration
FROM staging_songs_table;
""")

artist_table_insert = ("""
INSERT INTO artist_table (d_artist_id, d_name, d_location, d_lattitude, d_longitude)
SELECT DISTINCT 
        artist_id AS d_artist_id,
        artist_name AS d_name,
        artist_location AS d_location,
        artist_latitude AS d_lattitude,
        artist_longitude AS d_longitude
FROM staging_songs_table;
""")

time_table_insert = ("""
INSERT INTO time_table (d_start_time, d_hour, d_day, d_week, d_month, d_year, d_weekday)
SELECT 
    (timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_start_time,
    EXTRACT(HOUR FROM timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_hour,
    EXTRACT(DAY FROM timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_day,
    EXTRACT(WEEK FROM timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_week,
    EXTRACT(MONTH FROM timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_month,
    EXTRACT(YEAR FROM timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_year,
    EXTRACT(DOW FROM timestamp 'epoch' + ts::numeric * interval '0.001 seconds') AS d_weekday
FROM staging_events_table
WHERE ts IS NOT NULL;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
