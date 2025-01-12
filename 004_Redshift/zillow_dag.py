from airflow import DAG
from datetime import timedelta, datetime
import json
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# define our default args
default_args = {
    'owner': 'mdesafari',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 11),
    'email': ['mydataengineeringjourney@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=3)
}

# now date string
now = datetime.now()
now_date_string = now.strftime("%d%m%Y%H%M%S")

# get the api header
with open('/home/ubuntu/airflow/config_api.json', 'r') as f:
    header = json.load(f)

# define our slack_alert function
def slack_alert(context):
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
        channel='#zillowdatapipeline'
    )

    # return the context
    return send_slack_notification.execute(context=context)

# define our etl function
def etl_function(**kwargs):
    # get the args
    url = kwargs['url']
    querystring = kwargs['querystring']
    headers = kwargs['headers']
    date_string = kwargs['date_string']

    # get the data
    r = requests.get(url, headers=headers, params=querystring)
    data = r.json()

    # save the data as a JSON file in EC2
    # 1. create the file path
    filename = f'data_{date_string}'
    output_filepath = f'/home/ubuntu/{filename}.json'

    # 2. save it
    with open(output_filepath, 'w') as out_file:
        json.dump(data, out_file, indent=4)  # indent for pretty formatting
    
    # create a csv filename (we will use it latter - you will understand latter)
    csv_filename = f'{filename}.csv'

    # return both paths
    return [output_filepath, csv_filename]


# let's define our dag
with DAG('zillow_analytics_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        on_failure_callback=slack_alert,  # a function we will define
        catchup=False) as dag:

        # task 1: Extract real estate data (from rapid api), transform and save it on EC2 as CSV file
        extract_transform_save_on_ec2 = PythonOperator(
            task_id='extract_transform_save_on_ec2',
            python_callable=etl_function,
            # these are args we will pass to our 'etl_function' function
            op_kwargs={
                'url': "https://zillow56.p.rapidapi.com/search",
                'querystring': {"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"},
                'headers': header,
                'date_string': now_date_string
            }  
        )

        # task 2: Move the JSON file to S3 bucket (s3-bucket-landing-zone)
        # remember, 'task 1' return a list [output_filepath, csv_filename]
        # we want to move our json file (which is output_filepath) to s3
        # 
        move_json_file_to_s3_landing_zone = BashOperator(
            task_id='move_json_file_to_s3_landing_zone',
            bash_command='aws s3 mv {{ ti.xcom_pull("extract_transform_save_on_ec2")[0] }} s3://s3-bucket-landing-zone/'
        )

        # task 3: check if the csv file is available in our s3-bucket-transformed-data
        final_bucket_name = 's3-bucket-transformed-data'
        is_csv_file_available_in_s3_bucket_transformed_data = S3KeySensor(
            task_id='is_csv_file_available_in_s3_bucket_transformed_data',
            # filename which is in 'task 1' => [output_filepath, csv_filename]
            bucket_key='{{ ti.xcom_pull("extract_transform_save_on_ec2")[1]}}',
            bucket_name=final_bucket_name,
            aws_conn_id='conn_aws_id', # let's create it to no forgot
            wildcard_match=False,  # Set this to True if you want to use wildcards in the prefix
            timeout=60,  # Optional: Timeout for the sensor (in seconds)
            poke_interval=5,  # Optional: Time interval between S3 checks (in seconds)
        )

        # task 4: transfer the data (csv) to Amazon Redshift Warehouse
        # first let's create our Redshift instance
        transfer_data_from_s3_to_redshift = S3ToRedshiftOperator(
            task_id='transfer_data_from_s3_to_redshift',
            aws_conn_id='conn_aws_id',
            redshift_conn_id='conn_redshift_id', # we will create it shortly
            s3_bucket=final_bucket_name,
            s3_key='{{ ti.xcom_pull("extract_transform_save_on_ec2")[1]}}',
            schema="PUBLIC",
            table="realestate",
            copy_options=['csv IGNOREHEADER 1']  # 1: true
        )

        # connect the tasks
        extract_transform_save_on_ec2 >> move_json_file_to_s3_landing_zone
        move_json_file_to_s3_landing_zone >> is_csv_file_available_in_s3_bucket_transformed_data
        is_csv_file_available_in_s3_bucket_transformed_data >> transfer_data_from_s3_to_redshift