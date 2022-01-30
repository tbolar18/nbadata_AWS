import boto3




def create_roster_table(client,resoruce):
    try:
        table = client.create_table(
            TableName='Roster',
            KeySchema=[
                {
                    'AttributeName': 'playername',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'teamname',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'playername',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'teamname',
                    'AttributeType': 'S'
                },

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
    except ddb_exceptions.ResourceInUseException:
        print("Table Exists")


if __name__ == '__main__':
    client = boto3.client(
        'dynamodb',
        aws_access_key_id='AKIAVWFDYEZFMZAQKNJX',
        aws_secret_access_key='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',
    )
    resource = boto3.resource(
        'dynamodb',
        aws_access_key_id='AKIAVWFDYEZFMZAQKNJX',
        aws_secret_access_key='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',
    )
    ddb_exceptions = client.exceptions
    roster_table = create_roster_table(client,ddb_exceptions)