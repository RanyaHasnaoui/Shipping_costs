from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone  
import requests
import json

#fetch today's wind data
def fetch_weather_data(execution_date):

    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    today_str = execution_date.strftime('%Y-%m-%d')
    headers = {"Authorization": Variable.get("weather_api_key")}
    url = 'https://api.stormglass.io/v2/weather/point?lat=40.05&lng=7.84&params=windSpeed'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    filtered_hours = [hour for hour in response.json().get('hours', []) if hour['time'].startswith(today_str)]
    with open('/tmp/source1.json', 'w') as f:
        json.dump({'hours': filtered_hours}, f)

#fetch today's brent prices data
def fetch_crude_oil_data(execution_date):

    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    today_str = execution_date.strftime('%Y-%m-%d')
    url = f'https://api.eia.gov/v2/petroleum/pri/spt/data/?frequency=daily&data[0]=value&facets[series][]=RBRTE&start={today_str}&end={today_str}&api_key={Variable.get("crude_api_key")}'
    response = requests.get(url)
    response.raise_for_status()
    with open('/tmp/source2.json', 'w') as f:
        json.dump(response.json(), f)

#fetch today's exchange rates usd % tnd
def fetch_currency_data(execution_date):
    """Fetch today's exchange rates."""
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    today_str = execution_date.strftime('%Y-%m-%d')
    url = f'https://v6.exchangerate-api.com/v6/{Variable.get("currency_api_key")}/latest/TND'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    data["fetched_date"] = today_str
    with open('/tmp/source3.json', 'w') as f:
        json.dump(data, f)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 8, tzinfo=timezone.utc),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='api_daily_data_pipeline',
    default_args=default_args,
    schedule='0 0 * * *',  # midnight UTC daily
    catchup=False,
    max_active_runs=1,
    tags=['daily', 'api'],
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        op_kwargs={'execution_date': '{{ ds }}'},  
    )

    fetch_crude = PythonOperator(
        task_id='fetch_crude_oil_data',
        python_callable=fetch_crude_oil_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    fetch_currency = PythonOperator(
        task_id='fetch_currency_data',
        python_callable=fetch_currency_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    clean_data = BashOperator(
        task_id='clean_data_with_spark',
        bash_command='/opt/spark/bin/spark-submit /opt/airflow/pyspark/data_cleaning.py'
    )

    fetch_weather >> fetch_crude >> fetch_currency >> clean_data