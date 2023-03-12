import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE if NOT EXISTS staging_events 
        (
            artist           VARCHAR,
            auth             VARCHAR,
            first_name       VARCHAR,
            gender           VARCHAR,
            item_in_session  INTEGER,
            last_name        VARCHAR,
            length           FLOAT,
            level            VARCHAR,
            location         VARCHAR,
            method           VARCHAR,
            page             VARCHAR,
            registration     FLOAT8,
            session_id       INTEGER,
            song             VARCHAR,
            status           INTEGER,
            ts               BIGINT,
            user_agent       VARCHAR,
            user_id          VARCHAR
        );
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE if NOT EXISTS staging_songs 
        (
            num_songs         INTEGER,
            artist_id         VARCHAR,
            artist_latitude   FLOAT,
            artist_longitude  FLOAT,
            artist_location   VARCHAR,
            artist_name       VARCHAR,
            song_id           VARCHAR,
            title             VARCHAR,
            duration          FLOAT,
            year              INTEGER
        );
    """
)

songplay_table_create = (
    """
    CREATE TABLE if NOT EXISTS songplay
        (
            songplay_id  INTEGER IDENTITY(0,1) PRIMARY KEY,
            start_time   TIMESTAMP NOT NULL SORTKEY,
            user_id      VARCHAR NOT NULL DISTKEY,
            level        VARCHAR,
            song_id      VARCHAR NOT NULL,
            artist_id    VARCHAR,
            session_id   INTEGER,
            location     VARCHAR,
            user_agent   VARCHAR
        ) diststyle key;
    """
)

user_table_create = (
    """
    CREATE TABLE if NOT EXISTS users
        (
            user_id     VARCHAR PRIMARY KEY SORTKEY,
            first_name  VARCHAR,
            last_name   VARCHAR,
            gender      VARCHAR,
            level       VARCHAR
        ) diststyle all;
    """
)

song_table_create = (
    """
    CREATE TABLE if NOT EXISTS song
        (
            song_id    VARCHAR PRIMARY KEY SORTKEY,
            title      VARCHAR,
            artist_id  VARCHAR NOT NULL DISTKEY,
            year       INTEGER,
            duration   FLOAT
        ) diststyle key;
    """
)

artist_table_create = (
    """
    CREATE TABLE if NOT EXISTS artist
        (
            artist_id  VARCHAR PRIMARY KEY SORTKEY,
            name       VARCHAR,
            location   VARCHAR,
            latitude   FLOAT,
            longitude  FLOAT
        ) diststyle all;
    """
)

time_table_create = (
    """
    CREATE TABLE if NOT EXISTS time
        (
            start_time  TIMESTAMP PRIMARY KEY SORTKEY,
            hour        INTEGER,
            day         INTEGER,
            week        INTEGER,
            month       INTEGER,
            year        INTEGER DISTKEY,
            weekday     INTEGER
        ) diststyle key;
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    JSON {}
    region 'us-west-2'
    """
).format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    JSON 'auto'
    region 'us-west-2'
    """
).format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplay
    (
        start_time, user_id, level, song_id,
        artist_id, session_id, location, user_agent
    )
    SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + (1530402820197192::bigint::float / 1000000) * INTERVAL '1 second',   
           e.user_id, e.level, s.song_id, s.artist_id,
           e.session_id, e.location, e.user_agent 
    FROM staging_events AS e JOIN staging_songs AS s ON 
    e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration
    WHERE e.page = 'NextSong'
    """
)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT user_id, first_name, last_name, gender, level FROM staging_events
    """
)

song_table_insert = (
    """
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration FROM staging_songs 
    """
)

artist_table_insert = (
    """
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
    FROM staging_songs
    """
)

time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    WITH datetime AS( SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + (1530402820197192::bigint::float / 1000000) * INTERVAL '1 second' AS ts FROM staging_events)
    SELECT ts, extract(hour FROM ts), 
    extract(day FROM ts), extract(week FROM ts), extract(month FROM ts), 
    extract(year FROM ts), extract(weekday FROM ts) FROM datetime
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
