import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import python_operator

#
#   TODO: Define a function for the PythonOperator to call
#

def greet():
    logging.info("Hello, World!")

dag = DAG(
    'lesson1.demo1',
    description="Prints Hello, World caption",
    start_date=datetime.datetime.now()
)

#
#   TODO: Uncomment the operator below and replace the arguments labeled <REPLACE> below
#

greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)

/opt/airflow/start.sh 

# Instructions
# Complete the TODOs in this DAG so that it runs once a day. Once you’ve done that, open the Airflow UI using the "Access Airflow" button. go to the Airflow UI and turn the last exercise off, then turn this exercise on. Wait a moment and refresh the UI to see Airflow automatically run your DAG.

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")

#
# TODO: Add a daily `schedule_interval` argument to the following DAG
#
dag = DAG(
    "lesson1.exercise2",
    start_date=datetime.datetime.now() - datetime.timedelta(days=60),
    schedule_interval="@monthly"
    )
    
task = PythonOperator(
    task_id="hello_world_task",
    python_callable=hello_world,
    dag=dag)
