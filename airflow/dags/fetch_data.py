from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from airflow.models import Variable
from airflow.operators.bash import BashOperator

#fetch wind weather
def fetch_weather_data():
    
    weather_api_key = Variable.get("weather_api_key")  
    headers = {
        "Authorization": weather_api_key
    }
    url = 'https://api.stormglass.io/v2/weather/point?lat=40.05&lng=7.84&params=windSpeed' # Set the latitude and longitude for the zone you want to work with.(Hint: try looking this up on the map!)
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    
    with open('/tmp/source1.json', 'w') as f:
        json.dump(data, f)

#fetch brent prices
def fetch_crude_oil_data():

    crude_api_key = Variable.get("crude_api_key")
    url = (
        f'https://api.eia.gov/v2/petroleum/pri/spt/data/?frequency=daily&data[0]=value&facets[series][]=RBRTE&start=2025-01-01&api_key={crude_api_key}'
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    with open('/tmp/source2.json', 'w') as f:
        json.dump(data, f)

#fetch currency rates 
def fetch_currency_data():

    currency_api_key = Variable.get("currency_api_key")
    url = f'https://v6.exchangerate-api.com/v6/{currency_api_key}/latest/TND'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    with open('/tmp/source3.json', 'w') as f:
        json.dump(data, f)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 8),
    'retries': 1,
}

with DAG(
    dag_id='api_data_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    task1 = PythonOperator(
       task_id='fetch_weather_data',
       python_callable=fetch_weather_data,
   )
    task2 = PythonOperator(
        task_id='fetch_crude_oil_data',
        python_callable=fetch_crude_oil_data,
    )
    task3 = PythonOperator(
        task_id='fetch_currency_data',
        python_callable=fetch_currency_data,
    )
    spark_clean_task = BashOperator(
    task_id='clean_data_with_spark',
    bash_command='/opt/spark/bin/spark-submit /opt/airflow/pyspark/data_cleaning.py'
)


    task1 >> task2 >> task3 >> spark_clean_task
