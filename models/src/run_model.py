import boto3
import json
from sagemaker.tensorflow.serving import Model


# _model_data = "s3://sagemaker-us-east-1-433390365361/model/model.tar.gz"
# _role = 'SagemakerAdmin'


def get_and_process_images(bucket, prefix, predictor):
    # Create a client
    client = boto3.client('s3', region_name='us-east-1')

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
#     bucket = "sagemaker-us-east-1-433390365361"
    pages = paginator.paginate(Bucket=bucket, RequestPayer='requester')

    positive_list = []
    for page in pages:
        for obj in page['Contents']:
            response = client.get_object(Bucket=bucket, Key=obj['Key'], RequestPayer='requester')
            data = response['Body']
            results = run_model(predictor, data)
            if results == 'yes': # need to check this
                positive_list.append(obj['Key'])
    return positive_list


# def deploy_model(model_data, role):
#     model = Model(model_data=model_data, role=role)
#     predictor = model.deploy(initial_instance_count=1, instance_type='ml.c5.xlarge', accelerator_type='ml.eia1.medium')
#     return predictor


def run_model(predictor, data):
    # predictor = model.deploy(initial_instance_count=1, instance_type='ml.c5.xlarge')
    return 'yes'


# {
#   "Records": [
#     {
#       "Sns": {
#         "Message": "{ "s3Buckets: [ "s3://…1B_EW_GRDM_1SDH_20191202T155715_20191202T155815_019188_0243A1_8786", "s3://…" ] }"
#       }
#     }
#   ]
# }
def handler(snsEvent):
    for record in snsEvent["Records"]:
      messageObj = json.loads(record["Sns"]["Message"])
      for bucket in messageObj["s3Buckets"]:
        prefix = "GRD/2019/12/2/EW/DH/S1B_EW_GRDM_1SDH_20191202T155715_20191202T155815_019188_0243A1_8786/measurement"
        get_and_process_images(bucket, prefix, predictor=None)
