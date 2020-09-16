import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN=config.get('IAM_ROLE', 'ARN')
LOG_DATA=config.get('S3', 'LOG_DATA')
LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
SONG_DATA=confg.get('S3', 'SONG_DATA')
SONGS_JSONPATH=config.get('S3', 'SONGS_JSONPATH')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exits songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exits time"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists staging_events (
        event_id int identity(0,1) not null,
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    ); 
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int,
    );
""")

songplay_table_create = ("""
    create table if not exists songplays (
        songplay_id int identity(0,1) primary key,
        start_time timestamp not null,
        user_id varchar not null,
        level varchar,
        song_id varchar not null,
        artist_id varchar not null,
        session_id int not null,
        location varchar,
        user_agent varchar
    ) DISTKEY (user_id) SORTKEY (songplay_id);
""")

user_table_create = ("""
    create table if not exists users (
        user_id varchar primary key,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    ) SORTKEY (user_id);
""")

song_table_create = ("""
    create table if not exits songs (
        song_id varchar primary key,
        title varchar,
        arist_id varchar not null,
        year int,
        duration float
    ) SORTKEY (song_id);
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id varchar primary key,
        name varchar,
        location varchar,
        latitude varchar,
        longitude varchar
    ) SORTKEY (artist_id);
""")

time_table_create = ("""
    create table if not exists time (
        start_time timestamp primary key,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year smallint,
        weekday smallint
    ) SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-east-1'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json {}
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-east-1'
""").format(SONG_DATA, ARN, SONGS_JSONPATH)

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    select distinct
        timestamp 'epoch' + stgev.ts / 1000 * interval '1 second' as start_time,
        stgev.user_id,
        stgev.level,
        stgsng.song_id,
        stgsng.artist_id,
        stgev.sesstion_id,
        stgev.location,
        stgev.user_agent
    from staging_events as stgev
        join staging_songs as stgsng 
        on (stgev.artist = stgsng.artist_name)
    where stgev.page = 'NextSong';

""")

user_table_insert = ("""
    insert into users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    select distinct
        stgev.user_id,
        stgev.first_name,
        stgev.last_name,
        stgev.gender,
        stgev.level
    from staging_events as stgev
    where stgev.page = 'NextSong';
""")

song_table_insert = ("""
    insert into songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    select distinct
        stgsng.song_id,
        stgsng.title,
        stgsng.artist_id,
        stgsng.year,
        stgsng.duration
    from staging_songs as stgsng;
""")

artist_table_insert = ("""
    insert into artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    select distinct
        stgsng.artist_id,
        stgsng.artist_name,
        stgsng.artist_location,
        stgsng.artist_latitude,
        stgsng.artist_longitude
    from staging_songs as stgsng;
""")

time_table_insert = ("""
    insert into time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    selct distinct
        timestamp 'epoch' + stgev.ts / 1000 * interval '1 second' as start_time,
        extract (hour from start_time) as hour,
        extract (day from start_time) as day,
        extract (week from start_time) as week,
        extract (month from start_time) as month,
        extract (year from start_time) as year,
        extract (weekday from start_time) as weekday
    from staging_events as stgev
    where stgev.pge = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
