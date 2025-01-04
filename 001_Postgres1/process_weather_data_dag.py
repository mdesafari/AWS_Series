from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
import json
import pandas as pd
from datetime import datetime, timedelta


# set up default arguments for your dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# credentials
city_name = 'Chicago'
api_key = '...'

# create our functions
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return round(temp_in_fahrenheit, 3)


def transform_load_data(task_instance):
    # data comes from paralGroup.extract_weather_data
    data = task_instance.xcom_pull(task_ids='paralGroup.extract_weather_data')

    # transform the data
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # create a dictionary
    transformed_data = {"city": city,
                        "description": weather_description,
                        "temperature_farenheit": temp_farenheit,
                        "feels_like_farenheit": feels_like_farenheit,
                        "minimun_temp_farenheit":min_temp_farenheit,
                        "maximum_temp_farenheit": max_temp_farenheit,
                        "pressure": pressure,
                        "humidity": humidity,
                        "wind_speed": wind_speed,
                        "time_of_record": time_of_record,
                        "sunrise_local_time":sunrise_time,
                        "sunset_local_time": sunset_time                        
                        }
    
    # create a dataframe
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # save data on ec2
    df_data.to_csv('current_weather_data.csv', index=False, header=False)


def load_weather():
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    hook.copy_expert(
        sql="COPY weather_data FROM stdin WITH DELIMITER as ','",
        filename='current_weather_data.csv'
    )


def save_data_into_s3(task_instance):
    data = task_instance.xcom_pull(task_ids='join_tables')
    
    # build a dataframe from the data
    df_data = pd.DataFrame(data, columns=['city', 'state', 'land_Area_sq_mile_2020', 'description', 'temperature_farenheit', 'feels_like_farenheit'])

    # let's now save it to s3
    # 1. create our filename
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_weather_data_' + dt_string

    # 2. save it to s3
    df_data.to_csv(f's3://s3-postgres-database-2/{dt_string}.csv', index=False)

# create our dag
with DAG('parallel_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    # start
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    with TaskGroup(group_id='paralGroup', tooltip='parallel_processing') as paralGroup:
        ### FIRST BLOCK
        # 1. create table A
        create_table_a = PostgresOperator(
            task_id='create_table_a',
            postgres_conn_id='postgres_conn',
            sql='''
                CREATE TABLE IF NOT EXISTS city_look_up (
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                census_2020 numeric NOT NULL,
                land_Area_sq_mile_2020 numeric NOT NULL
            );
            '''
        )

        # 2. remove rows of table A if it contains data
        truncate_table = PostgresOperator(
            task_id='truncate_table',
            postgres_conn_id='postgres_conn',
            sql='''TRUNCATE TABLE city_look_up;
            '''
        )

        # 3. upload data from our s3 bucket (us_city.csv) and store it into our postgres table (table A = city_look_up)
        upload_s3_data_to_postgres = PostgresOperator(
            task_id='upload_s3_data_to_postgres',
            postgres_conn_id='postgres_conn',
            sql='''
                SELECT aws_s3.table_import_from_s3('city_look_up', '', '(format csv, DELIMITER '','', HEADER true)', 's3-postgres-database-2', 'us_city.csv', 'us-east-2');
            '''
        )

        ### SECOND BLOCK
        # 1. create postgres table B (weather_data)
        create_table_b = PostgresOperator(
            task_id='create_table_b',
            postgres_conn_id='postgres_conn',
            sql='''
                CREATE TABLE IF NOT EXISTS weather_data (
                    city TEXT,
                    description TEXT,
                    temperature_farenheit NUMERIC,
                    feels_like_farenheit NUMERIC,
                    minimun_temp_farenheit NUMERIC,
                    maximum_temp_farenheit NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
            );
            '''
        )

        # 2. check if API for Chicago is ready
        is_api_ready = HttpSensor(
            task_id='is_api_ready',
            http_conn_id='weather_id',
            endpoint=f'/data/2.5/weather?q={city_name}&appid={api_key}'
        )

        # 3. extract weather data
        extract_weather_data = HttpOperator(
            task_id='extract_weather_data',
            http_conn_id='weather_id',
            endpoint=f'/data/2.5/weather?q={city_name}&appid={api_key}',
            method='GET',
            response_filter=lambda r: json.loads(r.text), # r is a json string, so it transform it to a python obj (e.g. dict)
            log_response=True
        )

        # 4. transform the data and store it first on ec2 as a csv file
        transform_load_data_on_ec2 = PythonOperator(
            task_id='transform_load_data_on_ec2',
            python_callable=transform_load_data  # we need to create this function
        )

        # 5. load data from ec2 (current_weather_data) to our postgres table B (weather_data)
        load_weather_data_from_ec2_to_s3_bucket = PythonOperator(
            task_id='load_weather_data_from_ec2_to_s3_bucket',
            python_callable=load_weather  # let's create this function
        )

        # connect
        create_table_a >> truncate_table >> upload_s3_data_to_postgres
        create_table_b >> is_api_ready >> extract_weather_data >> transform_load_data_on_ec2 >> load_weather_data_from_ec2_to_s3_bucket
    

    # out of the group
    # 1. join both postgres tables (city_look_up (table A) and weather_data (table B))
    join_tables = PostgresOperator(
        task_id='join_tables',
        postgres_conn_id='postgres_conn',
        sql='''
            SELECT
                w.city,
                c.state,
                c.land_Area_sq_mile_2020,
                description,
                temperature_farenheit,
                feels_like_farenheit
            FROM weather_data AS w
            INNER JOIN city_look_up AS c
            ON w.city = c.city
        ;
        '''
    )

    # 2. load the joined data into our s3 bucket
    load_joined_data_to_s3 = PythonOperator(
        task_id='load_joined_data_to_s3',
        python_callable=save_data_into_s3  # let's create this function
    )

    # 3. end pipeline
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # connect
    start_pipeline >> paralGroup >> join_tables >> load_joined_data_to_s3 >> end_pipeline