# boto3 is a AWS SDK for Python
# It allows interaction with AWS services like S3
import boto3
import json

# creates an S3 client object to interact with the Amazon S3 service.
s3_client = boto3.client('s3')

# Defines the AWS Lambda function's entry point.
# The event parameter contains data about the triggering event
# Context provides runtime information.
def lambda_handler(event, context):
    # Extracts the name of the S3 bucket where the event originated
    source_bucket = event['Records'][0]['s3']['bucket']['name']

    # Extracts the key (file name and path) of the object in the S3 bucket 
    # that triggered the event.
    object_key = event['Records'][0]['s3']['object']['key']

    # Specifies the name of the destination bucket where the object will be copied.
    target_bucket = 's3-bucket-intermediate-zone'

    # Creates a dictionary representing the source object to be copied,
    # including its bucket and key
    copy_source = {
        'Bucket': source_bucket,
        'Key': object_key
    }

    # Retrieves a waiter object to check if the object exists in the source bucket.
    waiter = s3_client.get_waiter('object_exists')

    # Waits until the specified object exists in the 
    # source bucket to ensure it is ready for copying.
    waiter.wait(Bucket=source_bucket, Key=object_key)

    # Copies the object from the source bucket to the 
    # target bucket using the specified parameters.
    s3_client.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)

    # Returns a JSON-formatted response indicating 
    # the successful completion of the copy operation
    return {
        'statusCode': 200,
        'body': json.dumps('Copy successfully completed')
    }
