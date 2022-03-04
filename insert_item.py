import boto3


def create_user():

    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('Users')

    for i in range(1, 2):
        user = {
            'id': i,
            'first_name': 'User First Name {}'.format(i),
            'last_name': 'User Last Name {}'.format(i),
            'email': 'userid{}@test.com'.format(i)
        }

        table.put_item(Item=user)


print('Creating users...')
create_user()
print('User created successfully')
