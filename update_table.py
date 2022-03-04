import boto3


def update():
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('Users')

    table.update_item(
        Key={
                'id': 1,
            },
        UpdateExpression="set first_name = :g",
        ExpressionAttributeValues={
                ':g': "Jane Ba"
            },
        ReturnValues="UPDATED_NEW"
        )


update()
