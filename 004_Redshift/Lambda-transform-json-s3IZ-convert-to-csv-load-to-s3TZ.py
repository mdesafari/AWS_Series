import boto3
import json
import pandas as pd  # we need to create a layer containing pandas library

# itialize s3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # get source bucket and file name
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']

    # get filename without json extension
    file_name_without_extension = file_name.split('.')[0]

    # specify the target bucket
    target_bucket = 's3-bucket-transformed-data'

    # build the target file name
    target_file_name = f'{file_name_without_extension}.csv'

    # get the waiter object
    waiter = s3_client.get_waiter('object_exists')

    # wait for the object to exist
    waiter.wait(Bucket=source_bucket, Key=file_name)

    # get the object from the source bucket
    response = s3_client.get_object(Bucket=source_bucket, Key=file_name)

    # get the data of interest
    data = json.loads(response['Body'].read().decode('utf-8'))

    # print the data
    print(data)

    # transform the data
    transformed_data = []
    for item in data['results']:
        transformed_data.append(item)

    # create the final dataframe
    data = pd.DataFrame(transformed_data)

    # Select specific columns
    selected_columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 
                    'homeType','livingArea','price', 'rentZestimate','zipcode']
    
    data = data[selected_columns]

    # print it again
    print(data)

    # now, convert the dataframe to csv
    csv_data = data.to_csv(index=False)

    # upload the data to our s3 target
    s3_client.put_object(Bucket=target_bucket, Key=target_file_name, Body=csv_data)

    return {
        'statusCode': 200,
        'body': json.dumps('CSV conversion and S3 upload completed successfully')
    }
