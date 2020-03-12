# DROP TABLES

songplay_table_drop = "drop table songplays"
user_table_drop = "drop table users"
song_table_drop = "drop table songs"
artist_table_drop = "drop table artists"
time_table_drop = "drop table time"

# CREATE TABLES

songplay_table_create = ("""
    create table songplays (
        songplay_id serial not null primary key, 
        start_time timestamp, 
        user_id varchar references users(user_id), 
        level varchar, 
        song_id varchar references songs(song_id), 
        artist_id varchar references artists(artist_id), 
        session_id int, 
        location varchar, 
        user_agent varchar
    )
""")

user_table_create = ("""
    create table users (
        user_id varchar not null primary key, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    )
""")

song_table_create = ("""
    create table songs (
        song_id varchar not null primary key, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float
    )
""")

artist_table_create = ("""
    create table artists (
        artist_id varchar not null primary key, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float
    )
""")

time_table_create = ("""
    create table time (
        start_time timestamp not null primary key, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday varchar
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    values (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    values (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    values (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    values (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
    select u.first_name, u.last_name, u.level, s.title, s.duration, a.name, a.location
    from songplays sp inner join users u on sp.user_id = u.user_id
    inner join songs s on sp.song_id = s.song_id
    inner join artists a on sp.artist_id = a.artist_id
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]