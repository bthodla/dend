# DROP TABLES

songplay_table_drop = "drop table songplays"
user_table_drop = "drop table users"
song_table_drop = "drop table songs"
artist_table_drop = "drop table artists"
time_table_drop = "drop table time"

# CREATE TABLES

songplay_table_create = ("""
    create table songplays (
        songplay_id int, 
        start_time timestamp, 
        user_id int, 
        level varchar, 
        song_id int, 
        artist_id int, 
        session_id int, 
        location varchar, 
        user_agent varchar
    )
""")

user_table_create = ("""
    create table users (
        user_id int, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    )
""")

song_table_create = ("""
    create table songs (
        song_id int, 
        title varchar, 
        artist_id int, 
        year int, 
        duration float
    )
""")

artist_table_create = ("""
    create table artists (
        artist_id int, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float
    )
""")

time_table_create = ("""
    create table time (
        start_time timestamp, 
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
    values (1001, '2020-03-02 19:10:25-07', 101, 'Free', 10001, 2001, 51, 'New York, NY', 'Mozilla/5.0 (Windows NT 6.1)')
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    values (101, 'John', 'Smith', 'Male', 'Paid Subscription (Annual)')
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration
    values (10001, 'Michelle', 2001, 1969, 222.1309)
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    values (51, 'The Beatles', 'Liverpool, London', 51.5173469, 0.0831336)
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    values (1541990258.796, 2, 12, 46, 11, 2018, 'Mon')
""")

# FIND SONGS

song_select = ("""
    select u.first_name, u.last_name, u.level, s.title, s.duration, a.name, a.location
    from songplays sp inner join users u on sp.user_id = u.user_id
    inner join songs s on sp.song_id = s.song_id
    inner join artists a on sp.artist_id = a.artist_id
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]