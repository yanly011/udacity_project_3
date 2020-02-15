import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop ="DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        event_id      INT8 IDENTITY(0,1)      NOT NULL,
        artist        VARCHAR                 NULL ,
        auth          VARCHAR                 NULL,
        firstName     VARCHAR                 NULL,
        gender        VARCHAR                 NULL,
        itemInSession INT4                   NULL,
        lastName      VARCHAR                 NULL,
        length        VARCHAR                 NULL,
        level         VARCHAR                 NULL,
        location      VARCHAR                 NULL,
        method        VARCHAR                 NULL,
        page          VARCHAR                 NULL,
        registration  VARCHAR                 NULL,
        sessionId     INTEGER                 NOT NULL DISTKEY,
        song          VARCHAR                 NULL,
        status        INTEGER                 NULL,
        ts            INT8                    NOT NULL SORTKEY,
        userAgent     VARCHAR                 NULL,
        userId        INTEGER                 NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs           INTEGER          NULL , 
        artist_id           VARCHAR          NOT NULL  sortkey distkey, 
        artist_latitude     VARCHAR          NULL, 
        artist_longitude    VARCHAR          NULL, 
        artist_location     VARCHAR(512)     NULL, 
        artist_name         VARCHAR          NULL, 
        song_id             VARCHAR          NOT NULL, 
        title               VARCHAR          NULL, 
        duration            DECIMAL(9)      NULL, 
        year                INT4             NULL
    );
""")

songplay_table_create = ("""
     CREATE TABLE songplay (
        songplay_id     VARCHAR           NOT NULL     sortkey, 
        start_time      VARCHAR           NULL, 
        user_id         INTEGER           NOT NULL     distkey, 
        level           VARCHAR           NOT NULL, 
        song_id         VARCHAR           NOT NULL, 
        artist_id       VARCHAR           NOT NULL, 
        session_id      VARCHAR           NOT NULL, 
        location        VARCHAR(256)      NULL, 
        user_agent      VARCHAR           NULL
    );
""")

user_table_create = ("""
     CREATE TABLE users (
         user_id        VARCHAR    NOT NULL    sortkey, 
         first_name     VARCHAR    NULL, 
         last_name      VARCHAR    NULL, 
         gender         VARCHAR    NULL,
         level          VARCHAR    NOT NULL
     )
diststyle all;
""")

song_table_create = ("""
     CREATE TABLE songs (
         song_id      VARCHAR        NOT NULL    sortkey, 
         title        VARCHAR        NULL, 
         artist_id    VARCHAR        NOT NULL, 
         year         INT2           NULL, 
         duration     DECIMAL(10)    NULL
    )
diststyle all;
""")

artist_table_create = ("""
     CREATE TABLE artists (
         artist_id    VARCHAR         NOT NULL    sortkey, 
         name         VARCHAR         NULL, 
         location     VARCHAR(512)    NULL, 
         latitude     DECIMAL(9,5)    NULL, 
         longitude    DECIMAL(9,5)    NULL
    ) 
diststyle all;
""")

time_table_create = ("""
     CREATE TABLE times (
         start_time    TIMESTAMP    NOT NULL    sortkey, 
         hour          INT2         NULL, 
         day           INT2         NULL, 
         week          INT2         NULL, 
         month         INT2         NULL, 
         year          INT2         NULL, 
         weekday       INT2         NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
     INSERT INTO songplay (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
     SELECT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time,
            e.userId AS user_id,
            e.level AS level,
            s.song_id AS song_id,
            s.artist_id AS artist_id,
            e.sessionId AS session_id,
            e.location AS location,
            e.userAgent as user_agent
     FROM events_staging e
     LEFT JOIN songs_staging s 
     ON e.song = s.title 
     AND e.artist = s.artist_name
     WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
     INSERT INTO users (user_id, first_name, last_name, gender, level) 
     SELECT DISTINCT e.userId    AS user_id,
            e.firstName          AS first_name,
            e.lastName           AS last_name,
            e.gender             AS gender,
            e.level              AS level
     FROM staging_events AS e
     WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
     INSERT INTO songs (song_id, title, artist_id, year, duration)
     SELECT DISTINCT s.song_id         AS song_id,
            s.title                    AS title,
            s.artist_id                AS artist_id,
            s.year                     AS year,
            s.duration                 AS duration
     FROM staging_songs AS s
""")

artist_table_insert = ("""
     INSERT INTO artists (artist_id, name, location, latitude, longitude) 
     SELECT DISTINCT s.artist_id       AS artist_id,
            s.artist_name              AS name,
            s.artist_location          AS location,
            s.artist_latitude          AS latitude,
            s.artist_longitude         AS longitude
     FROM staging_songs AS s;
""")

time_table_insert = ("""
     INSERT INTO times (start_time, hour, day, week, month, year, weekday) 
     SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS e
    WHERE   e.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
