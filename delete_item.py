import boto3


def delete_user():
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('Users')

    table.delete_item(
        Key={
            'id': 1,
        },
    )


delete_user()
