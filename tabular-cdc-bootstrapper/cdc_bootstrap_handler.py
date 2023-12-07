import json
import logging
import os
import traceback

# 💃 this extra spice is required to unzip lambda dependencies. 
# This is necessary because of the zip:true config in our serverless.yml 💃
try:
  import unzip_requirements
except ImportError:
  pass


import tabular


# Tabular ENVs
TABULAR_CREDENTIAL       = os.environ['TABULAR_CREDENTIAL']
TABULAR_CATALOG_URI      = os.environ['TABULAR_CATALOG_URI']
TABULAR_TARGET_WAREHOUSE = os.environ['TABULAR_TARGET_WAREHOUSE']

# S3 Monitoring ENVs
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_BUCKET_PATH = os.environ.get('S3_BUCKET_PATH', '') # monitor the whole bucket if no path provided


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle_new_file(event, context):
  # Extract S3 details from event
  s3_info = event['Records'][0]['s3']
  object_key = s3_info['object']['key']

  logger.info(f"""Processing new bootstrap event...
    TABULAR_CATALOG_URI: {TABULAR_CATALOG_URI}
    TABULAR_TARGET_WAREHOUSE: {TABULAR_TARGET_WAREHOUSE}
    S3_BUCKET_NAME: {S3_BUCKET_NAME}
    S3_BUCKET_PATH: {S3_BUCKET_PATH}

    Object Key: {object_key}

  """)

  try:
    # parquet-only gate
    if not object_key.endswith('.parquet'):
      raise ValueError("Only parquet files are supported right now, sunshine 🌞.")

    catalog_properties = {
      'uri':        TABULAR_CATALOG_URI,
      'credential': TABULAR_CREDENTIAL,
      'warehouse':  TABULAR_TARGET_WAREHOUSE
    }

    tabular.bootstrap_from_file(object_key, S3_BUCKET_PATH, catalog_properties)

        
  except Exception as e:
    error_message = str(e)
    error_type = type(e).__name__
    stack_info = traceback.format_exc()

    return {
        'statusCode': 500,
        'errorType': error_type,
        'errorMessage': error_message,
        'stackTrace': stack_info,
    }


  else:
    return {
      'statusCode': 200,
      'body': json.dumps('Looks good to me 🌞')
    }
