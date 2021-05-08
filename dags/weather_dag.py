from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from weather_pipeline.utils.app_logger import _get_logger
from weather_pipeline.tasks import filter_weather_stations, get_city_interval, ingest_load_cities, \
                ingest_load_weather_readings, ingest_load_weather_stations, \
                create_germany_medians

logger = _get_logger(__name__)

default_args = {
    "owner": "Prakhar Srivastava",
    "trigger_rule": "all_success"
}

with DAG('Germany_weather', default_args=default_args,
    description='A simple tutorial DAG',
    start_date=days_ago(1),
    tags=["TR TASK"]) as dag:
    
    task1 = PythonOperator(task_id='ingest_load_cities',
                        python_callable=ingest_load_cities)
    
    task2 = PythonOperator(task_id='ingest_load_weather_stations',
                        python_callable=ingest_load_weather_stations) 
    
    task3 = PythonOperator(task_id = "filter_weather_stations",
                        python_callable=filter_weather_stations)
    
    task4 = PythonOperator(task_id = "ingest_load_weather_readings_2020",
                        python_callable= ingest_load_weather_readings,
                        op_kwargs={"year":2020})
    
    task5 = PythonOperator(task_id = "ingest_load_weather_readings_2021",
                        python_callable= ingest_load_weather_readings,
                        op_kwargs={"year":2021})
    
    task6 = PythonOperator(task_id = "create_germany_medians",
                        python_callable=create_germany_medians)
    
    task7 = PythonOperator(task_id = "get_city_interval",
                    python_callable=get_city_interval,
                    op_kwargs={"city":"Berlin",
                            "date":"2021-03-31"})
    
    task1 >> task2 >> task3 >> [task4 , task5] >> task6 >> task7