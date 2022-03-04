import boto3


def scan_first_and_last_names():
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('Users')

    resp = table.scan(ProjectionExpression="id")

    print(resp['Items'])


scan_first_and_last_names()
