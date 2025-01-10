from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 9),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

bucket_key = 's3://snowflake-bucket-example/city_folder/us_city.csv'
bucket_name = None  # None because the bucket_key is in the form s3:// (see documentation)

# create our dag
with DAG('snowflake_with_email_notif',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        # task 1: Check if csv file is available in S3 bucket
        is_csv_file_in_s3_available = S3KeySensor(
            task_id='is_csv_file_in_s3_available',
            bucket_key=bucket_key,
            bucket_name=bucket_name,
            aws_conn_id='conn_aws_id', # to connect Airflow to S3
            wildcard_match=False, # set this to True if you want to use wildcards in the bucket_key
            poke_interval=3 # optional: time interval between S3 checks (in seconds)
        )

        # task 2: create snowflake table in our snowflake database
        create_snowflake_table = SnowflakeOperator(
            task_id='create_snowflake_table',
            snowflake_conn_id='conn_snow_id', # to connect Airflow to Snowflake
            sql='''
                DROP TABLE IF EXISTS city_info;
                CREATE TABLE IF NOT EXISTS city_info (
                    city TEXT NOT NULL,
                    state TEXT NOT NULL,
                    census_2020 numeric NOT NULL,
                    land_Area_sq_mile_2020 numeric NOT NULL
                );
            '''
        )

        # task 3: Copy the csv content (in S3) into the Snowflake table
        copy_csv_in_s3_into_snowflake_table = SnowflakeOperator(
            task_id='copy_csv_in_s3_into_snowflake_table',
            snowflake_conn_id='conn_snow_id',
            sql='''
                COPY INTO city_database.city_schema.city_info
                FROM @city_database.city_schema.city_stage
                FILE_FORMAT = our_csv_format;
            '''
        )

        # task 4: send a notification via email
        send_notification_by_email = EmailOperator(
            task_id='send_notification_by_email',
            to='mydataengineeringjourney@gmail.com',
            subject='S3 data successfully loaded into Snowflake table',
            html_content='Well done ! The data is loaded'
        )

        # let's connect both tasks
        is_csv_file_in_s3_available >> create_snowflake_table >> copy_csv_in_s3_into_snowflake_table
        copy_csv_in_s3_into_snowflake_table >> send_notification_by_email