import sys
import json
import logging
import os
import time
import uuid
from datetime import date, timedelta, datetime

import boto3
import urllib

dynamodb = boto3.resource('dynamodb')

table = "deltas3"
bucket = "sentinel-s1-l1c"
start_dt = date(2019, 11, 30)
end_dt = date(2019, 12, 2)

def lambda_handler(event, context):
    delta = end_dt - start_dt
    for i in range(delta.days + 1):
        date = str(start_dt + timedelta(days=i))
        items = date.split('-')
        year = items[0]
        month = items[1]
        day = items[2]
        results = checkS3(year, month, day)
        triggerSNS(results)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def checkS3(year, month, day):
    client = boto3.client('s3', region_name='us-east-1') #GRD/2019/12/2/EW/DH/S1B_EW_GRDM_1SDH_20191202T155715_20191202T155815_019188_0243A1_8786

    paginator = client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix="GRD/" + year +"/" + month + "/"+ day + "/EW/DH", RequestPayer='requester')

    results = set()

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                results.add('/'.join(obj['Key'].split('/')[0:7]))

    return results

def triggerSNS(items):
    json_str = { "s3Buckets": ["s3://" + bucket + "/" + item for item in items]}
    client = boto3.client('sns')
    response = client.publish(
        TargetArn="arn:aws:sns:us-east-1:433390365361:newImages",
        Message=json.dumps({'default': json.dumps(json_str)}),
        MessageStructure='json'
    )

