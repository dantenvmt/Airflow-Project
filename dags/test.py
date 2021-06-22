import pandas as pd
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
import csv
from pathlib import Path

def a():
    cars = {'Brand': ['Honda Civic','Toyota Corolla','Ford Focus','Audi A4'],
            'Price': [22000,25000,27000,35000]
            }

    df = pd.DataFrame(cars, columns= ['Brand', 'Price'])
    df.to_csv('export_dataframe.csv', index = False, header=True)
    print("File Path:", Path(__file__).absolute())
default_args = {
	'owner': 'default_user',
	'start_date': airflow.utils.dates.days_ago(1),
	'depends_on_past': False,
	  #With this set to true, the pipeline won't run if the previous day failed
	'email': ['dantenvmt@gmail.com'],
	'email_on_failure': True,
	 #upon failure this pipeline will send an email to your email set above
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes=5),
}
dag = DAG(
	'OO',
	default_args=default_args,
	#schedule_interval='00 20 * * *',
)

nani = PythonOperator(dag=dag,task_id='reeee', python_callable=a)
