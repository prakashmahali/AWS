import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')
    
    # Specify the source table name
    source_table = 'YourSourceTableName'
    
    # Create a timestamp for the backup table
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Define the backup table name
    backup_table = f'{source_table}_backup_{timestamp}'
    
    # Describe the source table to get its schema
    table_description = dynamodb.describe_table(TableName=source_table)
    
    # Get the key schema and attribute definitions from the source table
    key_schema = table_description['Table']['KeySchema']
    attribute_definitions = table_description['Table']['AttributeDefinitions']
    
    # Create the backup table with the same schema as the source table
    dynamodb.create_table(
        TableName=backup_table,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    
    # Wait for the backup table to become active
    waiter = dynamodb.get_waiter('table_exists')
    waiter.wait(TableName=backup_table)
    
    # Scan the source table and write items to the backup table
    response = dynamodb.scan(TableName=source_table)
    items = response['Items']
    
    for item in items:
        dynamodb.put_item(
            TableName=backup_table,
            Item=item
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Backup table {backup_table} created successfully.')
    }
