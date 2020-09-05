# Data Lake Project

## Project Scope

With an increase in the user base and number of songs in its database, Sparkify wants to move this data into a data lake. The current data resides in Amazon S3 in JSON format and includes the following:

- User Activity Logs
- Song Metadata

The scope of this project is to build an ETL pipeline that extracts this data from S3, process it using Spark and write the outputs back to S3 but in *paquet* format. As a part of the process, we should also split the data into facts and dimensions. And when writing all time-variant data, the data should be partitioned by time and any other appropriate attributes.

## Schema Design

The output will be classified into the following entities:

- Facts
    - songplays:
        - songplay_id
        - start_time
        - user_id
        - level
        - session_id
        - location
        - user_agent
        - song_id
        - artist_id
        - year
        - month

- Dimensions
    - artists:
        - artist_id
        - artist_name
        - artist_location
        - artist_latitude
        - artist_longitude
    - songs
        - title
        - artist_id
        - year
        - duration
    - time
        - start_time
        - hour
        - day
        - month
        - year
        - week
        - weekday
    - users
        - user_id
        - first_name
        - last_name
        - gender
        - level
        
I think that this structure normalizes the data adequately for handling most of the analytical queries that may be thrown at the data lake. 

For example, the single fact table contains references to primary keys of each of the dimension tables so that it may be joined with one or more of them.

## Possible Queries

Here is a list of some of the queries that may be answered by this design:

1. Most popular songs
2. Most popular artists
3. Users who stream the maximum number of songs in a given period
4. Artists with maximum number of songs
5. User counts by gender
6. Count of Free Vs. Paid users


    
