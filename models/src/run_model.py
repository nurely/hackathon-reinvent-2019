import json
import boto3
import uuid
import datetime
from time import sleep

modelVersion = '0.0.1'

def get_and_process_images(bucket, prefix, predictor):
    # Create a client
    client = boto3.client('s3', region_name='us-east-1')

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
#     bucket = "sagemaker-us-east-1-433390365361"
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, RequestPayer='requester')

    positive_list = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                print(obj['Key'])
                if obj['Key'].endswith('.tiff'): 
                    response = client.get_object(Bucket=bucket, Key=obj['Key'], RequestPayer='requester')
                    data = response['Body']
                    results = run_model(predictor, data)
                    if results == 'yes': # need to check this
                        positive_list.append(obj['Key'])
    print(positive_list)
    return positive_list

def run_model(predictor, data):
    # predictor = model.deploy(initial_instance_count=1, instance_type='ml.c5.xlarge')
    sleep(2)
    if next(data)[0]%2 == 1:
        return 'yes'
    else:
        return 'no'

def lambda_handler(snsEvent, context):
    print(snsEvent['Records'][0])
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('OilSpill')
    
    for record in snsEvent["Records"]:
      messageObj = json.loads(record["Sns"]["Message"])
      for bucket in messageObj["s3Buckets"]:
        baseBucket = bucket.split("//")[1].split("/")[0]
        prefix = '/'.join(bucket.split("//")[1].split("/")[1:]) + '/measurement'
        foundSpills = get_and_process_images(baseBucket, prefix, predictor=None)
        for spill in foundSpills:
          table.put_item(
            Item={
                  'id': str(uuid.uuid4()),
                  's3-key': spill,
                  'isSpill': 1,
                  'modelVersion': modelVersion,
                  'createdAt': str(datetime.datetime.now())
              }
          )

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps({ 'foundSpills': len(foundSpills) })
    }
