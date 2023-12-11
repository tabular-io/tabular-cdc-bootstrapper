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

# Tabular connectivity
TABULAR_CREDENTIAL       = os.environ['TABULAR_CREDENTIAL']
TABULAR_CATALOG_URI      = os.environ['TABULAR_CATALOG_URI']
TABULAR_TARGET_WAREHOUSE = os.environ['TABULAR_TARGET_WAREHOUSE']

# S3 Monitoring
S3_BUCKET_TO_MONITOR = os.environ['S3_BUCKET_TO_MONITOR']
S3_PATH_TO_MONITOR   = os.environ['S3_PATH_TO_MONITOR']
S3_MONITORING_URI = f's3://{S3_BUCKET_TO_MONITOR}/{S3_PATH_TO_MONITOR}'


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
    S3_MONITORING_URI: {S3_MONITORING_URI}

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

    if tabular.bootstrap_from_file(object_key, S3_MONITORING_URI, catalog_properties):
      msg = 'Table successfully bootstrapped ✅'
      logger.info(msg=msg)
      return {
        'statusCode': 200,
        'body': json.dumps(msg)
      }

    else:
      msg = 'Nothing to do 🌞'
      logger.info(msg=msg)
      return {
        'statusCode': 200,
        'body': json.dumps(msg)
      }

        
  except Exception as e:
    error_message = str(e)
    error_type = type(e).__name__
    stack_info = traceback.format_exc()

    resp = {
      'statusCode': 500,
      'errorType': error_type,
      'errorMessage': error_message,
      'stackTrace': stack_info,
    }

    logger.error(f'\nFailed to bootstrap 🔴\n{resp}')

    return resp
