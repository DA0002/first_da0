from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2
import json


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    lat = 36.976847
    lon = 127.913131
    key = context["params"]["api_key"]
    url = context["params"]["api"].format(lat =lat, lon=lon, key=key)
    #key = "2d61882f7e74a4787667e1de1e8004b1"
    #url = "https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={key}&units=metric".format(lat=lat, lon = lon, key=key)


    task_instance = context['task_instance']
    execution_date = context['execution_date']

    print(url)

    logging.info(execution_date)
    f = requests.get(url)
    return (f.json())


def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")

    return text["daily"]


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = "BEGIN; DROP TABLE IF EXISTS {schema}.{table}; CREATE TABLE {schema}.{table} (date date primary key, temp float, min_temp float, max_temp float);".format(schema=schema, table=table)
    for line in lines:
        date = datetime.fromtimestamp(line["dt"]).strftime('%Y-%m-%d')
        temp_day = line["temp"]["day"]
        temp_min = line["temp"]["min"]
        temp_max = line["temp"]["max"]
        print(date, temp_day, temp_min, temp_max)
        sql += f"""INSERT INTO {schema}.{table} VALUES ('{date}', '{temp_day}', '{temp_min}', '{temp_max}');"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


dag_second_assignment = DAG(
    dag_id = 'Weather_forecast',
    start_date = datetime(2023,4,6), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'api':  Variable.get("open_weather_api"),
        'api_key' : Variable.get("open_weather_api_key")
    },
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'olo127',   ## 자신의 스키마로 변경
        'table': 'weather_forecast'
    },
    dag = dag_second_assignment)

extract >> transform >> load
