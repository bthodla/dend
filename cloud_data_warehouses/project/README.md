# Data Warehoue Project

## Project Scope

Sparkify, the music streaming startup, has grown their user base and song database. They would like to move their processes and data into the cloud. Their data resides in S3 as follows:

- User activity on the app in JSON format in one directory 
- Song Metadata on the songs offered on their app in another directory

The scope of this project is to build an ETL pipeline that extracts this data from S3, stage it in Redshift and then convert them into a set of fact and dimenstional tables, again in Redshift. The expectation is that this will facilitate their analytical team to get new insights into usage patters of users.

## Schema Design

The output will be classified into the following entities:

- Staging Tables
    - Staging Events
        - event_id
        - artist
        - auth
        - firstName
        - gender
        - itemInSession
        - lastName
        - length
        - level
        - location
        - method
        - page
        - registration
        - sessionId
        - song
        - status
        - ts
        - userAgent
        - userId

    - Staging Songs
        - artist_id
        - artist_name
        - artist_location
        - artist_latitude
        - arist_longitude
        - song_id varchar
        - title
        - duration
        - year
        - num_songs

- Facts
    - songplays:
        - songplay_id
        - start_time
        - user_id
        - level
        - song_id
        - artist_id
        - session_id
        - location
        - user_agent

- Dimensions
    - artists:
        - artist_id
        - name
        - location
        - latitude
        - longitude
    - songs
        - song_id
        - title
        - artist_id
        - year
        - duration
    - time
        - start_time
        - hour
        - day
        - week
        - month
        - year
        - weekday
    - users
        - user_id
        - first_name
        - last_name
        - gender
        - level
        
I think that this structure normalizes the data adequately for handling most of the analytical queries that may be thrown at this cloud data warehouse.

For example, the single fact table contains references to primary keys of each of the dimension tables so that it may be joined with one or more of them.

## Possible Queries

Here is a list of some of the queries that may be answered by this design:

1. Most popular songs
2. Most popular artists
3. Users who stream the maximum number of songs in a given period
4. Artists with maximum number of songs
5. User counts by gender
6. Count of Free Vs. Paid users

## Code Structure

Each of the code files in this project is briefly described here:

**_sql_queries.py_**
    This sets up the statements for the following:
        - Drop tables if they exist to facilitate repetitive testing
        - Create the staging and analytical tables 
        - Copy JSON data from S3 into staging tables
        - Insert data from staging tables into analytical tables

**_create_tables.py_**
    This code is provided as a part of the project to help us run the statements in the "sql_queries.py" to do the steps described in the previous section
    
**_etl.py_**
    This sets up the ETL pipelines to read data from the JSON files and load them into the corresponding staging tables. It also sets up ETL pipelines to load the data from these staging tables into the analytical tables. The portion of the code that sets up the time data first breaks down the timestamp into its constituent elements and then inserts all of them as a record into the table.
 
    
