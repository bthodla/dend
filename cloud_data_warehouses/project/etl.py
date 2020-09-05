import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load input data in JSON format from Log Data and Song Data files stored in S3
    Insert the data into staging_events and staging_songs tables respectively
    Parameters:
        cur: Cursor from the database that we connected to
        conn: Connection to the Redshift database (Postgres)

    Expected behavior: 
        Data from Log Data files is stored in staging_events table
        Data from Song Data files is stored in staging_songs table
    """
    print("Begin loading data from S3 to Redshift tables")
    for query in copy_table_queries:
        print("Now processing query: {}".format(query))
        cur.execute(query)
        conn.commit()
        print("Completed processing query: {}".format(query))

    print("All files were successfully loaded into staging tables")


def insert_tables(cur, conn):
    """
    Insert data from staging tables into star schema-optimized analytical tables
    Sources: staging_events and staging_songs
    Destination:
        Fact Table(s): songplys
        Dimension Tables: artists, songs, time, users

    Parameters:
        cur: Cursor from the database that we connected to
        conn: Connection to the Redshift database (Postgres)

    Expected behavior: 
        Data from Staging Tables are inserted into the analytical tables successfully
    """

    print("Begin loading data from S3 to Redshift tables")
    for query in insert_table_queries:
        print("Now processing query: {}".format(query))
        cur.execute(query)
        conn.commit()
        print("Completed processing query: {}".format(query))

    print("All data from stating tables was successfully loaded into analytical tables")


def main():
    """
    Connect to an already created Redshift database
    Load staging tables from JSON input files
    Load data from staging tables into analytical tables

    Parameters for the Redshift database are contained in dwh.cfg under the section titled CLUSTER
    Important among these are:
        host:       Redshift cluster URL
        dbname:     Database name (also known as Schema Name) 
        user:       Id of the user that has privileges to perform operations on the database
        password:   User password
        port:       The database port for the connection

    Expected behavior: 
        Input data in JSON file format is successfully loaded into staging tables
        Data from staging tables is sucessfully loaded into analytical tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()