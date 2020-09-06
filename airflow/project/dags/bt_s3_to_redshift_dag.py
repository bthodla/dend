from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'bthodla',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 120,
    'email_on_retry': False
}

# Initializing DAG
dag = DAG('bt_s3_to_redshift_dag',
          default_args=default_args,
          description='Load data into Redshift and transform it with Airflow',
          schedule_interval='0 * * * *'
        )

# Creating a dummy operator as a placeholder for starting the workflow
start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

# Setting up custom staging operators to create staging tables
create_table_staging_events = StageToRedshiftOperator(
    task_id='create_table_staging_events',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_staging_events
)

create_table_staging_songs = StageToRedshiftOperator(
    task_id='create_table_staging_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_staging_songs
)

# Using the Postgres Operator to create Fact tables
create_table_songplays = PostgresOperator(
    task_id='create_table_songplays',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_songplays
)

# Using the Postgres Operator to create Dimension tables
create_table_artists = PostgresOperator(
    task_id='create_table_artists',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_artists
)

create_table_songs = PostgresOperator(
    task_id='create_table_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_songs
)

create_table_time = PostgresOperator(
    task_id='create_table_time',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_time
)

create_table_users = PostgresOperator(
    task_id='create_table_users',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_users
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
