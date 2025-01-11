from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import requests
import pandas as pd

# create our slack alert function
def slack_alert_at_fail(context):
    # here we are getting the info on the failed dag and task
    ti = context.get('task_instance')
    dag_name = context.get('task_instance').dag_id
    task_name = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url 
    dag_run = context.get('dag_run')

    # let's prepare the message to send to slack
    message = f"""
        :red_circle: Pipeline Failed.        
        *Dag*:{dag_name}
        *Task*: {task_name}
        *Execution Date*: {execution_date}
        *Task Instance*: {ti}
        *Log Url*: {log_url}
        *Dag Run*: {dag_run}        
    """ 

    # send the notification
    send_slack_notification = SlackWebhookOperator(
        task_id='send_slack_notification',
        slack_webhook_conn_id='conn_slack_id',
        message=message,
        channel='#weatherdatapipeline'
    )

    # return the context
    return send_slack_notification.execute(context=context)

# extract, transform, load on EC2 function
def etl_function():
    # api key
    api_key = '...'  # put your openweathermap api key here

    # city we want to get data from
    cities = ['Portland', 'Chicago', 'Houston']
    
    # base url
    base_url = 'https://api.openweathermap.org'

    # loop
    combined_data = pd.DataFrame()
    for city in cities:
        endpoint = f'/data/2.5/weather?q={city}&appid={api_key}'

        # full url
        full_url = base_url + endpoint

        # get the data using requests
        r = requests.get(full_url)
        data = r.json()  # to convert it into a python object

        # retrieve some fields
        city_name = city
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        # synthetize the data as dictionary
        transformed_data = {
            'city_name': city_name,
            'pressure': pressure,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'time_of_record': time_of_record,
            'sunrise_time': sunrise_time,
            'sunset_time': sunset_time
        }

        # create a pandas dataframe
        data = pd.DataFrame([transformed_data])

        # concatenate
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    
    # create the final dataframe path (where to save it)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_' + dt_string

    # save the dataframe on EC2
    combined_data.to_csv(f'{dt_string}.csv', index=False)

    # return the path where the data has been stored on your ubuntu
    output_file = f'/home/ubuntu/{dt_string}.csv'
    return output_file


default_args = {
    'owner': 'mdesafari',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 10),
    'email': ['mydataengineeringjourney@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=3)
}

# create our dag
with DAG('weather_slack_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        on_failure_callback=slack_alert_at_fail,
        catchup=False) as dag:

        # task 1: extract_transform_save_on_ec2  # let's deliberately create a failure
        extract_transform_save_on_ec2 = PythonOperator(
            task_id='extract_transform_save_on_ec2',
            python_callable=etl_function
        )

        # task 2: let's now move the stored data in EC2 to S3
        # remember, task1 return the file path output_file
        # so ti.xcom_pull(task_id) will get the data of 'task_id' => here it is output_file
        move_csv_file_to_s3 = BashOperator(
            task_id='move_csv_file_to_s3',
            bash_command='aws s3 mv {{ ti.xcom_pull("extract_transform_save_on_ec2") }} s3://s3-bucket-slack-example'
        )

        # task 3: send slack notification
        send_slack_notification_on_success = SlackWebhookOperator(
            task_id='send_slack_notification_on_success',
            slack_webhook_conn_id='conn_slack_id',
            message='Data successfully loaded in S3',
            channel='#weatherdatapipeline'
        )
    

        # let's connect both tasks
        extract_transform_save_on_ec2 >> move_csv_file_to_s3 >> send_slack_notification_on_success