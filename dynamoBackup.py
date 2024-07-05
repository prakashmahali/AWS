import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # List of DynamoDB table names to back up
    table_names = event['table_names']
    
    # S3 bucket name where the backups will be stored
    s3_bucket = os.environ['S3_BUCKET']
    
    # Create backups for each table
    for table_name in table_names:
        try:
            # Create DynamoDB backup
            backup_response = dynamodb.create_backup(
                TableName=table_name,
                BackupName=f"{table_name}-backup-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
            )
            
            # Get backup details
            backup_arn = backup_response['BackupDetails']['BackupArn']
            backup_creation_date_time = backup_response['BackupDetails']['BackupCreationDateTime'].strftime('%Y-%m-%d-%H-%M-%S')
            
            # Save backup details to S3
            backup_info = {
                'TableName': table_name,
                'BackupArn': backup_arn,
                'BackupCreationDateTime': backup_creation_date_time
            }
            
            s3.put_object(
                Bucket=s3_bucket,
                Key=f"dynamodb-backups/{table_name}-backup-{backup_creation_date_time}.json",
                Body=json.dumps(backup_info)
            )
            
            print(f"Backup for table {table_name} created successfully.")
        
        except Exception as e:
            print(f"Error backing up table {table_name}: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Backups completed successfully')
    }
