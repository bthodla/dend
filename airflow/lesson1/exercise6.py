# Instructions
# Similar to what you saw in the demo, copy and populate the trips table. Then, add another operator which creates a traffic analysis table from the trips table you created. Note, in this class, we won’t be writing SQL -- all of the SQL statements we run against Redshift are predefined and included in your lesson.

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements


def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))


dag = DAG(
    'lesson1.exercise6',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

create_table >> copy_task
copy_task >> location_traffic_task

/*
Conn ID: redshift
Conn Type: Postgres
Host: redshift-cluster-1.cdodedeqidlv.us-east-1.redshift.amazonaws.com:5439/bt-udacity
Schema: bt-udacity
Login: bthodla
Password: rsawsH0bb!t
Port: 5439
JDBC URL: jdbc:redshift://redshift-cluster-1.cdodedeqidlv.us-east-1.redshift.amazonaws.com:5439/bt-udacity
ODBC URL: Driver={Amazon Redshift (x64)}; Server=redshift-cluster-1.cdodedeqidlv.us-east-1.redshift.amazonaws.com; Database=bt-udacity

Redshift Cluster Identifier: redshift-bt-cluster-1
*/

http://packages.confluent.io/archive/5.4/confluent-5.4.2-2.12.tar.gz
