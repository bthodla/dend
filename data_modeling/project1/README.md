# Data Analysis Project for Sparkify

## Project Scope

The music startup Sparkify collects usage statistics of its streaming music service. Specifically, it collects data relating to songs played by its users. Unfortunately, this data is in no form to be used directly by the analsyts as this data is in JSON format.

Broadly, the JSON files fall into two categories:

- Data relating to songs
- Data relating song plays

The mandate of this project is review these JSON files and load them into a Postgres database so that the analysts may query them using SQL.

## Data Model

After reviewing the JSON files, we have identified the following entities that may be useful for analysts' use:

- Songs
- Artits
- Users
- Song Plays

The first three are dimensions while the last ("Song Plays") is a "fact" or "measure". In addition, we considered that it would be useful to break down the timestamp of each song play into its constituent elements such as hours, days, weeks, weekdays, months and years so that time-based analysis may be performed using the same.

The following section describes the data model for this project listing the entities (tables), their attributes (columns) and their datatypes.

### Songs
- song_id varchar (Primary Key)
- title varchar
- artist_id varchar
- year int
- duration float

### Artists
- artist_id varchar (Primary Key)
- name varchar
- location varchar
- latitude float
- longitude float

### Users
- user_id varchar (Primary Key)
- first_name varchar
- last_name varchar
- gender varchar
- level varchar

### Song Plays
- songplay_id serial (Primary Key)
- start_time timestamp
- user_id varchar references users(user_id)
- level varchar
- song_id varchar references songs(song_id)
- artist_id varchar references artists(artist_id)
- session_id int
- location varchar
- user_agent varchar

*The "references" clauses in the above structures indicate that these columns are foreign keys and reference the primary keys of the corresponding tables (and their columns). This setups up a declarative referential integrity in the database to ensure that any values loaded into this table already have a representation in the corresponding dimension tables.*

### Time
- start_time timestamp (Primary Key)
- hour int
- day int
- week int
- month int
- year int
- weekday varchar

This model has been arrived at after taking the structures through a normalization process conforming to the 3rd Normal Form.

## Code Structure

Each of the code files in this project are briefly described here:

**_sql_queries.py_**
    This sets up the statements for creating the tables in the database as well as providing templates to insert data into the tables

**_create_tables.py_**
    This code is provided as a part of the project to help us run the statements in the "sql_queries.py" to do the actual creation of the tables
    
**_etl.py_**
    This sets up the ETL pipelines to read data from the JSON files and load them into the corresponding tables. All the available files are read into a list and are processed one at a time to extract data from the same relevant to each of the tables which are then inserted into the corresponding table. The portion of the code that sets up the time data first breaks down the timestamp into its constituent elements and then inserts all of them as a record into the table.
    
## Typical Analytical Queries

We are listing below the typical queries that an analyst may ask of this database:

- Most popular songs
- Most popular artists
- Users who stream the maximum number of songs in a given period
- Artists with maximum number of songs
- User counts by gender
- Count of Free Vs. Paid users

    ~~~~sql
    -- Most popular songs
    select s.title, count (*)
    from songplays sp inner join songs s on sp.song_id = s.song_id
    group by s.title
    order by 2 desc
    ;
    ~~~~

    ~~~~sql
    -- Most popular artists
    select a.name, count (*)
    from songplays sp inner join artists a on sp.artist_id = a.artist_id
    group by a.name
    order by 2 desc
    ;
    ~~~~

    ~~~~sql
    -- Users who stream the maximum number of songs in a given period
    select u.first_name, u.last_name, count (*)
    from songplays sp inner join users u on sp.user_id = u.user_id
        inner join time t on sp.start_time = t.start_time
    where t.year = 2018
    group by u.first_name, u.last_name
    order by 3 desc
    ;
    ~~~~

    ~~~~sql
    -- Artists with maximum number of songs
    select a.first_name, a.last_name, count (*)
    from aritst a inner join songs s on a.artist_id = s.artist_id
    group by a.first_name, a.last_name
    order by 3 desc
    limit 1
    ;
    ~~~~

    ~~~~sql
    -- User counts by gender
    select gender, count (*)
    from users
    group by gender
    ;
    ~~~~~

    ~~~~sql
    -- Count of free vs paid users
    select level, count (distinct user_id)
    from songplays
    group by level
    ;
    ~~~~

