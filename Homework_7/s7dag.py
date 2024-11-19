from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
default_args = {
'owner': 'ValeriK',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2024, month=11, day=18).in_timezone('Europe/Moscow'),
'email': ['lera@lera.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}
#DAG3
dag1 = DAG('HomeWork6_Task2',
default_args=default_args,
description="Work_4",
catchup=False,
schedule_interval='0 7 * * *')
task31 = BashOperator(
task_id='Step_Work_4',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/lera/Homework/HW6/Home_3_4/task4.py',
dag=dag1)
#DAG4
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pandas.io import sql
import time
#pip install openpyxl
dag2 = DAG('AGanshin003',
default_args=default_args,
description="seminar_6",
catchup=False,
schedule_interval='0 8 * * *')
def hello(**kwargs):
  encoding="ISO-8859-1"
  print('Hello from {kw}'.format(kw=kwargs['my_keyword']))
  df=5+5
  print(df)
  df=pd.read_excel('/home/lera/s4_2.xlsx')
  con=create_engine("mysql://Airflow:1@localhost:33061/spark")
  print(df)
  df['долг'] = df['Платеж по основному долгу'].cumsum()
  df['проценты'] = df['Платеж по процентам'].cumsum()
  df['год'] = round(df['№']/12,1)
  df.to_sql('credit',con,schema='spark',if_exists='replace',index=False)
t2 = PythonOperator(
task_id='python3',
dag=dag2,
python_callable=hello,
op_kwargs={'my_keyword': 'Airflow 1234'}
)
dag11 = DAG( 'hello_world' , description= 'Hello World DAG' , 
          schedule_interval= '0 12 * * *' , 
          start_date=datetime( 2023 , 1 , 1
          ), catchup= False ) 

hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag11)
hello_file_operator = BashOperator(task_id= 'hello_file_task' , bash_command='sh /home/lera/s6.sh ', dag=dag11) 
skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag11) 

hello_operator >> hello_file_operator >> skipp_operator
#DAG5
dag3 = DAG('HomeWork7_Task1',
default_args=default_args,
description="HomeWork7_1",
catchup=False,
schedule_interval='0 7 * * *')
task51 = BashOperator(
task_id='Step_Work_7_1',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/lera/Homework/HW7/Task1.py',
dag=dag3)
#DAG6
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os

def openwear_get_temp(**kwargs):

    ti = kwargs['ti']
    city = "Baranovichi"
    api_key = "ceb53b6cc59c1700bf78b943e0749299"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    return round(float(response.json()['main']['temp'])-273.15, 2)


def openwear_check_temp(ti):
    temp = int(ti.xcom_pull(task_ids='Baranovichi_get_temperature'))
    print(f'Temperature now is {temp}')
    if temp >= 15:
        return 'Baranovichi_Temp_warm'
    else:
        return 'Baranovichi_Temp_cold'

with DAG(
        'Baranovichi_check_temperature_warm_or_cold',
        start_date=datetime(2024, 11, 18),
        catchup=False,
        tags=['HomeWork7_weather'],
) as dag:
    Baranovichi_get_temperature = PythonOperator(
        task_id='Baranovichi_get_temperature',
        python_callable=openwear_get_temp,
    )

    Baranovichi_check_temperature = BranchPythonOperator(
        task_id='Baranovichi_check_temperature',
        python_callable=openwear_check_temp,
    )

    Baranovichi_Temp_warm = BashOperator(
        task_id='Baranovichi_Temp_warm',
        bash_command='echo "It is warm"',
    )

    Baranovichi_Temp_cold = BashOperator(
        task_id='Baranovichi_Temp_cold',
        bash_command='echo "It is cold"',
    )

Baranovichi_get_temperature >> Baranovichi_check_temperature >> [Baranovichi_Temp_warm, Baranovichi_Temp_cold]














