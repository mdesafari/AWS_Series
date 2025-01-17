from airflow import DAG
from datetime import timedelta, datetime
import json
import pandas as pd  # install also pandas in your Linux EC2 instance
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

# default parameters
default_args = {
    'owner': 'mdesafari',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 13),
    'email': ['mydataengineeringjourney@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=3)
}

# now date string
now = datetime.now()
now_date_string = now.strftime("%d%m%Y%H%M%S")

# create our slack alert task
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


# define our function etl_function
def etl_function(**kwargs):
    # let's get the params
    url = kwargs['url']
    query_string = kwargs['querystring']
    headers = kwargs['headers']
    date_string = kwargs['date_string']

    # get the data
    response = requests.get(url, headers=headers, params=query_string)
    data = response.json()

    # organize the data
    transformed_data = []
    for item in data['results']:
        transformed_data.append(item)
    
    # create our dataframe
    data = pd.DataFrame(transformed_data)

    # select columns of interest
    columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 'homeType','livingArea','price', 'rentZestimate','zipcode']
    data = data[columns]

    # save the data on EC2
    filename = f'data_{date_string}'
    filepath = f'/home/ubuntu/{filename}.csv'
    data.to_csv(filepath, index=False)

    # filename with extension
    filename_with_extension = f'{filename}.csv'

    # return
    return [filename_with_extension, filepath]


def transfer_data_s3_to_redshift(**kwargs):
    # get the job name
    job_name = kwargs['job_name']

    # create a session
    session = AwsGenericHook(aws_conn_id='aws_s3_conn')  # let's create the connection

    # get a client in the same region as the Glue job
    boto3_session = session.get_session(region_name='us-east-2')

    # trigger the job using its name
    client = boto3_session.client('glue')
    client.start_job_run(
        JobName=job_name,
    )


def get_run_id(**kwargs):
    # get the job name
    job_name = kwargs['job_name']

    # create a session
    session = AwsGenericHook(aws_conn_id='aws_s3_conn')  # let's create the connection

    # get a client in the same region as the Glue job
    boto3_session = session.get_session(region_name='us-east-2')
    glue_client = boto3_session.client('glue')

    # get the response
    response = glue_client.get_job_runs(JobName=job_name)

    # get the id
    job_run_id = response["JobRuns"][0]["Id"]

    return job_run_id


# kwargs
url = "https://zillow56.p.rapidapi.com/search"
query_string = {"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"}
with open('/home/ubuntu/airflow/config.json', 'r') as f:
    headers = json.load(f)

# create our dag
with DAG('zillow_analytics_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        on_failure_callback=slack_alert,  # a function we will define => for slack alert
        catchup=False) as dag:

        # task 1: retrieve zillow data from rapid api, transform it, and save it as CSV file on EC2
        extract_transform_save_on_ec2 = PythonOperator(
            task_id='extract_transform_save_on_ec2',
            python_callable=etl_function,  # let's define our function
            # these are args we will pass to our 'etl_function' function
            op_kwargs={
                'url': url,
                'querystring': query_string,
                'headers': headers,
                'date_string': now_date_string
            }
        )

        # task 2: move the csv file saved on EC2 to our s3 bucket
        # remember, 'task 1' return a list [filename_with_extension, filepath]
        # we want to move our csv file (which is filepath) to s3
        move_csv_to_s3_bucket = BashOperator(
            task_id='move_csv_to_s3_bucket',
            bash_command='aws s3 mv {{ ti.xcom_pull("extract_transform_save_on_ec2")[1]}} s3://s3-bucket-zillow/'
        )

        # task 3: check if the csv file is available in our s3-bucket before crawling
        is_csv_file_available_in_s3 = S3KeySensor(
            task_id='is_csv_file_available_in_s3',
            # bucket_key = filename with extension => [filename_with_extension, filepath]
            bucket_key='{{ ti.xcom_pull("extract_transform_save_on_ec2")[0] }}',
            bucket_name="s3-bucket-zillow",
            aws_conn_id="conn_aws_id",  # let's create it
            wildcard_match=False,  # Set this to True if you want to use wildcards in the prefix
            timeout=60,  # Optional: Timeout for the sensor (in seconds)
            poke_interval=5,  # Optional: Time interval between S3 checks (in seconds)
        )

        # task 4: trigger Glue Job (source -> change schema -> target)
        # load data from s3 to redshift
        trigger_glue_job = PythonOperator(
            task_id='trigger_glue_job',
            python_callable=transfer_data_s3_to_redshift,
            op_kwargs={'job_name': 's3_data_uploaded_into_redshift_glueJob'}
        )

        # task 5: grab glue job run id (in order to monitor its success or not)
        get_glue_job_run_id = PythonOperator(
            task_id='get_glue_job_run_id',
            python_callable=get_run_id,
            op_kwargs={'job_name': 's3_data_uploaded_into_redshift_glueJob'}
        )

        # task 6: ensure that glue job finish running
        is_glue_job_finish_running = GlueJobSensor(
            task_id='is_glue_job_finish_running',
            job_name='s3_data_uploaded_into_redshift_glueJob',
            run_id='{{ ti.xcom_pull("get_glue_job_run_id") }}',
            verbose=True,  # prints glue job logs in airflow logs
            aws_conn_id='aws_s3_conn',
            poke_interval=60,
            timeout=3600
        )

        # task 7: send notification to slack for successfully transfer data to redshift
        send_slack_notification_after_redshift = SlackWebhookOperator(
            task_id='send_slack_notification_after_redshift',
            slack_webhook_conn_id='conn_slack_id',
            message=f""":blue_circle: Pipeline Succeeded.""",
            channel='#zillowdatapipeline'
        )

        # connect both tasks
        extract_transform_save_on_ec2 >> move_csv_to_s3_bucket >> is_csv_file_available_in_s3
        is_csv_file_available_in_s3 >> trigger_glue_job >> get_glue_job_run_id
        get_glue_job_run_id >> is_glue_job_finish_running >> send_slack_notification_after_redshift

