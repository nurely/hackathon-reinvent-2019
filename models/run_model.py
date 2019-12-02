from sagemaker.tensorflow.serving import Model


model_data = "s3://sagemaker-us-east-1-433390365361/model/model.tar.gz"
role = 'SagemakerAdmin'




def deploy_model(model_data, role):
    model = Model(model_data=model_data, role=role)
    predictor = model.deploy(initial_instance_count=1, instance_type='ml.c5.xlarge', accelerator_type='ml.eia1.medium')
    return predictor


def run_model(predictor, data):
    predictor = model.deploy(initial_instance_count=1, instance_type='ml.c5.xlarge')


if __file__ == '__main__':
    predictor = deploy_model(model_data, role)
    run_model(predictor, data)
