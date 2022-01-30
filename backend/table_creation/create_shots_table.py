import boto3




def create_roster_table(client, exceptions):
    try:
        table = client.create_table(
            TableName='ShotsDetails',
            KeySchema=[
                {
                    'AttributeName': 'key',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'key',
                    'AttributeType': 'S'
                }

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("Creating table")
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName='Roster')
        print("Table created")
        return table
    except exceptions.ResourceInUseException:
        print("Table Exists")


if __name__ == '__main__':
    
    #keeping my key hidden
    client = boto3.client(
        'dynamodb',
        aws_access_key_id='****',
        aws_secret_access_key='****',
    )
    resource = boto3.resource(
        'dynamodb',
        aws_access_key_id=='****',
        aws_secret_access_key=='****',
    )
    ddb_exceptions = client.exceptions
    shots_table = create_roster_table(client,ddb_exceptions)
