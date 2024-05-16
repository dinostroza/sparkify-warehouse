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

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist             text ,
        auth               varchar(25),
        firstName          text,
        gender             varchar(1),
        itemInSession      integer,
        lastName           text,
        length             numeric,
        level              varchar(25),
        location           text,
        method             varchar(10),
        page               varchar(25) sortkey,
        registration       numeric,
        sessionId          integer,
        song               text,
        status             integer,
        ts                 timestamp,
        userAgent          text,
        userId             integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id          varchar(25) sortkey,
        artist_latitude    numeric,
        artist_location    text,
        artist_longitude   numeric,
        artist_name        text,
        duration           numeric,
        num_songs          integer,
        song_id            text,
        title              text,
        year               integer

    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id   bigint identity(0, 1),
        start_time    timestamp not null sortkey,
        user_id       integer not null distkey,
        level         varchar(15) not null,
        song_id       varchar(20),
        artist_id     varchar(20),
        session_id    integer not null,
        location      text,
        user_agent    text not null,
        PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id       integer        not null sortkey,
        first_name    text           not null,
        last_name     text           not null,
        gender        varchar(10)    not null,
        level         varchar(15)    not null,
        PRIMARY KEY (user_id)
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id      varchar(20)    not null sortkey,
        title        text           not null,
        artist_id    text           not null,
        year         int            not null,
        duration     numeric        not null,
        PRIMARY KEY (song_id)
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id     varchar(20)    not null sortkey,
        name          text           not null,
        location      text,
        latitude      numeric,
        longitude     numeric,
        PRIMARY KEY (artist_id)
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time    timestamp   not null sortkey,
        hour          smallint    not null,
        day   	      smallint    not null,
        week   	      smallint    not null,
        month      	  smallint    not null,
        year          smallint    not null,
        weekday       smallint    not null,
        PRIMARY KEY (start_time)
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}' 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json '{}'
    timeformat 'epochmillisecs';
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    copy staging_songs from '{}' 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent      
)                 
SELECT 
    se.ts, 
    se.userId, 
    se.level, 
    ss.song_id, 
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
LEFT JOIN staging_songs ss
ON se.song = ss.title
AND se.artist = ss.artist_name
AND se.length = ss.duration
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level 
    )
    WITH uniq_staging_events AS (
        SELECT userId, firstName, lastName, gender, level,
               ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
        FROM staging_events
        WHERE userid IS NOT NULL
    )
    SELECT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM uniq_staging_events 
    WHERE rank = 1;
""")

song_table_insert = ("""
    INSERT INTO songs (
         song_id,
         title,
         artist_id,
         year,
         duration
    )
    SELECT DISTINCT
         song_id,
         title,
         artist_id,
         year,
         duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    WITH uniq_staging_songs AS (
        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
               ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY year DESC) AS rank
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    )
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM uniq_staging_songs
    WHERE rank = 1;
""")



time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(ts)              AS start_time,
       EXTRACT(hour FROM ts)     AS hour,
       EXTRACT(day FROM ts)      AS day,
       EXTRACT(week FROM ts)     AS week,
       EXTRACT(month FROM ts)    AS month,
       EXTRACT(year FROM ts)     AS year,
       EXTRACT(weekday FROM ts)   AS weekday
FROM staging_events
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]