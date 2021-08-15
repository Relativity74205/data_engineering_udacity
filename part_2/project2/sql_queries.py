import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# For both staging tables diststyle key was chosen with the artist_name as dist key, as both tables are joined i.a. on the artist_name in an insert query
staging_events_table_create = """
CREATE TABLE staging_events (
artist VARCHAR (256) DISTKEY,
auth VARCHAR (16) NOT NULL,
firstName VARCHAR (128),
gender VARCHAR (1),
itemInSession INT NOT NULL,
lastName VARCHAR (128),
length FLOAT,
level VARCHAR (8) NOT NULL,
location VARCHAR (256),
method VARCHAR (16) NOT NULL,
page VARCHAR (32) NOT NULL,
registration FLOAT,
sessionId INT NOT NULL,
song VARCHAR (512),
status INT NOT NULL,
ts BIGINT NOT NULL,
userAgent VARCHAR (1024),
userId VARCHAR (32) NOT NULL
)
DISTSTYLE KEY
"""

staging_songs_table_create = """
CREATE TABLE staging_songs (
num_songs INT NOT NULL,
artist_id VARCHAR (18) NOT NULL,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR (256),
artist_name VARCHAR (256) NOT NULL DISTKEY,
song_id VARCHAR (18) NOT NULL,
title VARCHAR (512) NOT NULL,
duration NUMERIC NOT NULL,
year INT NOT NULL
)
DISTSTYLE KEY
"""

# as the songplay table is a fact table, diststyle even was chosen
songplay_table_create = """
CREATE TABLE songplays (
songplay_id INT IDENTITY(0,1) NOT NULL, 
start_time TIMESTAMP NOT NULL SORTKEY,
user_id INT NOT NULL,
level VARCHAR (8) NOT NULL,
song_id VARCHAR (18),
artist_id VARCHAR (18),
session_id INT NOT NULL,
location VARCHAR (256),
user_agent VARCHAR (1024)
)
DISTSTYLE EVEN;
"""

# as the users table is a dimension table, diststyle all was chosen
user_table_create = """
CREATE TABLE users (
user_id INT NOT NULL,
first_name VARCHAR (128) NOT NULL,
last_name VARCHAR (128) NOT NULL,
gender VARCHAR (1) NOT NULL,
level VARCHAR (8) NOT NULL 
)
DISTSTYLE ALL;
"""

# as the songs table is a dimension table, diststyle all was chosen
song_table_create = """
CREATE TABLE songs (
song_id VARCHAR (18) NOT NULL,
title VARCHAR (512) NOT NULL,
artist_id VARCHAR (18) NOT NULL,
year INT NOT NULL,
duration NUMERIC NOT NULL
)
DISTSTYLE ALL;
"""

# as the artists table is a dimension table, diststyle all was chosen
artist_table_create = """
CREATE TABLE artists (
artist_id VARCHAR (18) NOT NULL,
name VARCHAR (256) NOT NULL,
location VARCHAR (256),
latitude FLOAT,
longitude FLOAT
)
DISTSTYLE ALL;
"""

# as the time table is a dimension table, diststyle all was chosen
time_table_create = """
CREATE TABLE time (
start_time TIMESTAMP,
hour INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
month INT NOT NULL,
year INT NOT NULL,
weekday INT NOT NULL
)
DISTSTYLE ALL;
"""

# STAGING TABLES
staging_events_copy = f"""
COPY staging_events FROM {config['S3']['LOG_DATA']} 
CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
region 'us-west-2'
json {config['S3']['LOG_JSONPATH']}
;
"""

staging_songs_copy = f"""
COPY staging_songs FROM {config['S3']['SONG_DATA']}
CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
region 'us-west-2'
json 'auto'
;
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
    SELECT
        timestamp 'epoch' + se.ts/1000 * interval '1 second',
        CAST(se.userId AS INT),
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se

    LEFT JOIN staging_songs ss
    ON ss.title = se.song
    AND ss.artist_name = se.artist
    AND ss.duration = se.length
    
    WHERE se.page = 'NextSong'
)
;
"""

# The last value for the level attribute of a user is chosen. The distinct keyword removes duplicates.
user_table_insert = """
INSERT INTO users (
    SELECT DISTINCT
        CAST(se.userId AS INT),
        se.firstName,
        se.lastName,
        se.gender,
        FIRST_VALUE(se.level) OVER (PARTITION BY se.userId ORDER BY se.ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    FROM staging_events se
    
    WHERE se.page = 'NextSong'
);
"""

# The distinct keyword removes duplicates. The join with the target table prevent duplicates in case of a delta job.
song_table_insert = """
INSERT INTO songs (
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
    LEFT JOIN songs s USING(song_id)
    WHERE s.song_id IS NULL
);
"""

# The distinct keyword removes duplicates. The join with the target table prevent duplicates in case of a delta job.
artist_table_insert = """
INSERT INTO artists (
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
    LEFT JOIN artists a USING(artist_id)
    WHERE a.artist_id IS NULL
);
"""

# The distinct keyword removes duplicates. The join with the target table prevent duplicates in case of a delta job.
time_table_insert = """
INSERT INTO time (
    SELECT DISTINCT
        timestamp 'epoch' + se.ts/1000 * interval '1 second' as converted_ts,
        EXTRACT(HOUR from converted_ts),
        EXTRACT(DAY from converted_ts),
        EXTRACT(WEEK from converted_ts),
        EXTRACT(MONTH from converted_ts),
        EXTRACT(YEAR from converted_ts),
        date_part(dow, converted_ts)

    FROM staging_events se
    LEFT JOIN time t ON se.ts = t.start_time
    WHERE t.start_time IS NULL
);
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
