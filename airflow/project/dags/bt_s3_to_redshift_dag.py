from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY=os.environ.get('AWS_KEY')
# AWS_SECRET=os.environ.get('AWS_SECRET')

default_args={
    'owner': 'bthodla',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'catchup': False,
    'email_on_retry': False
}

# Initializing DAG
dag=DAG(
        'bt_s3_to_redshift_dag',
        default_args=default_args,
        description='Load data into Redshift and transform it with Airflow',
        schedule_interval='0 * * * *'
    )

# Creating a dummy operator as a placeholder for starting the workflow
start_operator=DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

stage_events_to_redshift=StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table='staging_events',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift=StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    table='staging_songs',
    copy_options="FORMAT AS JSON 'auto'"
)

load_songplays_table=LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    insert_query='SqlQueries.songplay_table_insert'
)

load_user_dimension_table=LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    insert_query='SqlQueries.user_table_insert',
    mode='truncate'
)

load_song_dimension_table=LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    insert_query='SqlQueries.song_table_insert',
    mode='truncate'
)

load_artist_dimension_table=LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    insert_query='SqlQueries.artist_table_insert',
    mode='truncate'
)

load_time_dimension_table=LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    insert_query='SqlQueries.time_table_insert',
    mode='truncate'
)

run_quality_checks=DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator=DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table

load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
