import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from sqlalchemy import create_engine
import pandas as pd
from tabulate import tabulate


os.environ["no_proxy"]="*"

@dag(
    dag_id="wether-tlegram-home",
    schedule="@once",
    start_date=pendulum.datetime(2024, 11, 23, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def WetherETL():

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_default',
        token='7672923175:AAE48c-RP3irUOP96hEaFuKvIRwGWmdPTaE',
        chat_id='5174647902',
        text='Wether in Baranovichi \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]['temperature']}}" + " degrees at " + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]['datetime']}}" +
    "\nOpen wether: " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]['temperature']}}" + " degrees at " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]['datetime']}}",
    )

    @task(task_id='yandex_wether')
    def get_yandex_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=53.132363&lon=26.017618"

        payload={}
        headers = {
        'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        temperature = response.json()['fact']['temp']
        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        a=response.json()['fact']['temp']
        print(a)
        ti.xcom_push(key='wether', value={'temperature': temperature, 'datetime': current_datetime})
        
    @task(task_id='open_wether')
    def get_open_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.openweathermap.org/data/2.5/weather?lat=53.132363&lon=26.017618&appid=2cd78e55c423fc81cebc1487134a6300"

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        temperature = round(float(response.json()['main']['temp']) - 273.15, 2)
        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        a=round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        ti.xcom_push(key='open_wether', value={'temperature': temperature,'datetime': current_datetime})

    
    @task(task_id='save_weather')
    def get_save_weather(**kwargs):
        yandex_data = kwargs['ti'].xcom_pull(task_ids='yandex_wether', key='wether')
        open_weather_data = kwargs['ti'].xcom_pull(task_ids='open_wether', key='open_wether')
        
        temperature_yandex = yandex_data['temperature']
        datetime_yandex = yandex_data['datetime']
        service_yandex = 'Yandex'
        
        temperature_open_weather = open_weather_data['temperature']
        datetime_open_weather = open_weather_data['datetime']
        service_open_weather = 'OpenWeather'
        
        engine = create_engine("mysql://root:1@localhost:33061/spark")
        
        with engine.connect() as connection:
            connection.execute("""DROP TABLE IF EXISTS spark.`Temperature_Weather`""")
            connection.execute("""CREATE TABLE IF NOT EXISTS spark.`Temperature_Weather` (
                Service VARCHAR(255),
                Date_time TIMESTAMP,
                City VARCHAR(255),
                Temperature FLOAT,
                PRIMARY KEY (Date_time, Service)
            )COLLATE='utf8mb4_general_ci' ENGINE=InnoDB""")
            connection.execute(f"""INSERT INTO spark.`Temperature_Weather` (Date_time, City, Temperature, Service) VALUES ('{datetime_yandex}', 'Baranovichi', {temperature_yandex}, '{service_yandex}')""")
            connection.execute(f"""INSERT INTO spark.`Temperature_Weather` (Date_time, City, Temperature, Service) VALUES ('{datetime_open_weather}', 'Baranovichi', {temperature_open_weather}, '{service_open_weather}')""")


    @task(task_id='python_wether')
    def get_wether(**kwargs):
        print("Yandex "+str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'],key='wether')[0])+" Open "+str(kwargs['ti'].xcom_pull(task_ids=['open_wether'],key='open_wether')[0]))

    
    get_yandex_wether() >> get_open_wether() >> get_wether() >> send_message_telegram_task >> get_save_weather()
    
    
dag = WetherETL()