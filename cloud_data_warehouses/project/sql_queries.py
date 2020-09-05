import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
        event_id int identity(0,1) not null ,
        artist varchar,
        auth varchar,
        first_name varchar,
        last_name varchar,
        gender varchar,
        item_in_session int,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        session_id int,
        song varchar,
        status int,
        ts timestamp not null,
        user_agent varchar,
        user_id int
    );
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs (
        artist_id varchar not null,
        artist_name varchar,
        artist_location varchar,
        artist_latitude varchar,
        arist_longitude varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int,
        num_songs int
    );
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
