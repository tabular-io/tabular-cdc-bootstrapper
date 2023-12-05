import json
import logging
import os

# ENVs
TABULAR_CREDENTIAL       = os.environ['TABULAR_CREDENTIAL']
TABULAR_CATALOG_URI      = os.environ['TABULAR_CATALOG_URI']
TABULAR_TARGET_WAREHOUSE = os.environ['TABULAR_TARGET_WAREHOUSE']

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle_new_file(event, context):
    logger.info('Function loaded successfully')
    
    # Extract S3 details from event
    s3_info = event['Records'][0]['s3']
    
    bucket_name = s3_info['bucket']['name']
    object_key = s3_info['object']['key']
    event_time = event['Records'][0]['eventTime']
    
    logger.info(f'Bucket: {bucket_name}')
    logger.info(f'Object Key: {object_key}')
    logger.info(f'Event Time: {event_time}')

    return {
        'statusCode': 200,
        'body': json.dumps('Function executed')
    }
