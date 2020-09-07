import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the existing tables from the database. This allows us to test creating tables multiple times.

    Parameters:
        cur: Cursor from the database that we connected to
        conn: Connection to the Redshift database (Postgres)

    Expected behavior: All the tables are dropped from the Redshift database as identified in drop_table_queries
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error:
            print("Error: Errors from dropping tables: " + query)
            print(error)

    print("All tables listed in drop_table_queries successfully dropped")


def create_tables(cur, conn):
    """
    Create new tables in the database based on create statements in create_table_queries

    Parameters:
        cur: Cursor from the database that we connected to
        conn: Connection to the Redshift database (Postgres)

    Expected behavior: All the tables identified in create_table_queries are successfully created
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error:
            print("Error: Errors creating tables: " + query)
            print(error)

    print("All tables listed in create_table_queries successfully created")


def main():
    """
    Connect to an already created Redshift database
    Drop tables if any are existing as identified in drop_table_queries
    Create tables if they don't already exist as identiied in create_table_queries

    Parameters for the Redshift database are contained in dwh.cfg under the section titled CLUSTER
    Important among these are:
        host:       Redshift cluster URL
        dbname:     Database name (also known as Schema Name) 
        user:       Id of the user that has privileges to perform operations on the database
        password:   User password
        port:       The database port for the connection

    Expected behavior: Drop existing tables if any and create new tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
