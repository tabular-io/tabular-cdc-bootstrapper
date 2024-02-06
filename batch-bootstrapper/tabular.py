import json
import logging

import requests

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError
from pyiceberg.table import Table

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_tabular_token(base_url: str, tabular_credential: str) -> str:
  """Gets a usable REST bearer token from a tabular credential

  Args:
      base_url (str): typically https://app.tabular.io, but varies with deployment tier
      tabular_credential (str): member or service account credential created in Tabular

  Returns:
      str: bearer token for API REST requests to Tabular
  """
  client_id, client_secret = tabular_credential.split(':')
  url = f"{base_url}/ws/v1/oauth/tokens"
  data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
  }
  headers = {'Content-Type': 'application/x-www-form-urlencoded'}

  resp = requests.post(url, headers=headers, data=data)
  if resp.status_code != 200:
    raise Exception(f"Failed to get token: {resp.content}")

  return resp.json()['access_token']

def get_cdc_target_table_properties(cdc_id_field: str, cdc_timestamp_field: str) -> dict:
  """
  generates the appropriate tabular properties dictionary for a tabular cdc target table

  Args:
    - cdc_id_field (str): column in the table representing the unique identity of each row in the cdc output. Often an id.
        For example: 'customer_id'. This tells tabular whether to update or insert a row.

    - cdc_timestamp_field (str): column in the table representing the timestamp in the current timestamp to use to determine
        which records belong to different points in time, specifically which records are the latest.
        For example: 'last_updated_at'. 
  """
  if not cdc_id_field or not cdc_timestamp_field:
    raise ValueError(f"""
      CDC target tables must have a non empty cdc_id_field and cdc_timestamp_field, but got the following
        - cdc_id_field        = "{cdc_id_field}"
        - cdc_timestamp_field = "{cdc_timestamp_field}"
    """) 

  properties = {}
  properties['etl.job-type']   = 'cdc'
  properties['cdc.type']       = 'DMS'
  properties['cdc.ts-column']  = cdc_timestamp_field
  properties['cdc.key-column'] = cdc_id_field

  return properties

def get_file_loader_target_table_properties(file_loader_s3_uri: str) -> dict:
  """
  generates the appropriate tabular properties dictionary for an iceberg table 
  requiring file loading and cdc processing. 

  Args:
    - file_loader_s3_uri (str): s3 uri that should be monitored for new files to load.
        For example: s3://{bucket_name}/{monitoring_path}
  """
  if not file_loader_s3_uri or not file_loader_s3_uri.startswith('s3://'):
    raise ValueError(f"""file_loader_s3_uri must exist and start with "s3://", but got "{file_loader_s3_uri}" """) 

  # https://docs.tabular.io/tables#file-loader-properties
  properties = {}
  properties['fileloader.enabled']       = 'true'
  properties['fileloader.path']          = file_loader_s3_uri
  properties['fileloader.file-format']   = 'parquet'
  properties['fileloader.write-mode']    = 'append'
  properties['fileloader.evolve-schema'] = 'true'

  return properties

def bootstrap_table(
  tabular_credential: str,
  tabular_base_url: str,
  org_id: str,
  warehouse_id: str,
  database_name: str,
  database_id: str,
  table_name: str,
  s3_uri: str,
  enable_fileloader: bool,
  file_exclusion_filter: str,
  catalog
  ):
  # see if the table exists
  try:
    target_table = catalog.load_table(f'{database_name}.{table_name}')
    logger.info(f"""
    Success(ish) - Existing table already found in catalog. Lets do nothing and move on...
      database_name: {database_name}
      table_name: {table_name}
    """)

    return # if the table exists, we're done here 😎

  except NoSuchTableError as nste:
    # get to boot strappin! 💪
    logger.info(f"""
      Creating table...
        tabular_credential: nice try, not logging a secret pal
        tabular_base_url: {tabular_base_url}
        org_id: {org_id}
        warehouse_id: {warehouse_id}
        database_name: {database_name}
        database_id: {database_id}
        table_name: {table_name}
        s3_uri: {s3_uri}
        enable_fileloader: {enable_fileloader}
        file_exclusion_filter: {file_exclusion_filter}
    """)
    
    auth_token = get_tabular_token(tabular_base_url, tabular_credential)
    
    url = f"{tabular_base_url}/v1/organizations/{org_id}/warehouses/{warehouse_id}/databases/{database_id}/tables"

    headers = {
      'accept': '*/*',
      'Authorization': f'Bearer {auth_token}',
      'Content-Type': 'application/json'
    }
    
    bucket, path = s3_uri[5:].split('/', 1)
    mode = 'CREATE_AUTO_LOAD' if enable_fileloader else 'CREATE_LOAD'

    data = {
      "tableName": table_name,
      "bucket": bucket,
      "prefixes": [path],
      "mode": mode,
      "fileLoaderConfig": {
        "fileFormat": 'parquet',
        "fileFilter": file_exclusion_filter
      }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
      raise Exception(f"Failed to execute query: {response.content}")
