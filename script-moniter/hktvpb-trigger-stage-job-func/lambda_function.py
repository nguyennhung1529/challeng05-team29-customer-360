import json
import boto3
import urllib.parse
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    
    # Parse the S3 event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        # Determine which Glue job to trigger based on the subfolder
        parts = key.split("/")
        if len(parts) > 2 and parts[0] == "raw_data":
            table_name = parts[1]
            job_name = f'hktvpb-raw-{table_name}-to-stage-{table_name}'
            try:
                # Start the Glue job
                response = glue_client.start_job_run(JobName=job_name)
                logger.info(f'Started Glue job: {job_name}')
            except glue_client.exceptions.EntityNotFoundException:
                logger.error(f'Glue job not found: {job_name}')
            except Exception as e:
                logger.error(f'Error starting Glue job: {job_name}, Error: {str(e)}')
        else:
            logger.warning(f'Unexpected S3 key format: {key}')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete.')
    }
