from io import BytesIO
import logging

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def extract_database_and_table(s3_object_path: str, s3_monitoring_path: str = '', is_dir: bool = False):
  """
  Extract the database and table name from the s3 object path, relative to 
  the s3_monitoring_path -> /some-path-to-monitor/{database}/{table_name}
  """
  if not s3_object_path.startswith(s3_monitoring_path):
    raise ValueError(f"""
      s3_object_path must exist within s3_monitoring_path:
      - s3_object_path:     "{s3_object_path}"
      - s3_monitoring_path: "{s3_monitoring_path}"
    """)

  relevant_s3_path = s3_object_path[len(s3_monitoring_path):].strip('/')
  logger.info(f"""
    Extracting database and table name from:
      - s3_object_path:     "{s3_object_path}"
      - s3_monitoring_path: "{s3_monitoring_path}"
      - relevant_path: "{relevant_s3_path}"
  """)

  try:
    # ignore the actual file by dropping the last element (-1 index) if this isn't a directory path
    relevant_parent_folders = relevant_s3_path.split('/')[:-1] if not is_dir else relevant_s3_path.split('/')
    database = relevant_parent_folders[0]
    table = relevant_parent_folders[1]

    return database, table

  except IndexError:
    raise ValueError("The s3 key must have at least 2 subdirectory levels.")

def get_tabular_table_properties(file_loader_s3_uri: str, cdc_id_field: str = '', cdc_timestamp_field: str = '') -> dict:
  """
  generates the appropriate tabular properties dictionary for an iceberg table requiring file loading
  and cdc processing. 

  Args:
    - file_loader_s3_uri (str): s3 uri that should be monitored for new files to load.
        For example: s3://{bucket_name}/{monitoring_path}

    - cdc_id_field (str): column in the table representing the unique identity of each row in the cdc output. Often an id.
        For example: 'customer_id'. This tells tabular whether to update or insert a row.

    - cdc_timestamp_field (str): column in the table representing the timestamp in the current timestamp to use to determine
        which records belong to different points in time, specifically which records are the latest.
        For example: 'last_updated_at'. 
  """
  properties = {}

  if file_loader_s3_uri:
    # https://docs.tabular.io/tables#file-loader-properties
    properties['fileloader.enabled']       = 'true'
    properties['fileloader.path']          = file_loader_s3_uri
    properties['fileloader.file-format']   = 'parquet'
    properties['fileloader.write-mode']    = 'append'
    properties['fileloader.evolve-schema'] = 'true'

  return properties

def bootstrap_cdc_target(
  s3_object_path: str, 
  s3_monitoring_path:str, 
  s3_bucket_name: str,  
  catalog_properties
) -> str:
  """
  Connects to an iceberg rest catalog with catalog_properties and bootstraps
  a new table if one doesn't already exist

  Args:
    - s3_object_path (str): The S3 object path to the target to process.
    - s3_monitoring_path (str): The S3 path being monitored for bootstrapping.
    - s3_bucket_name (str): The name of the S3 bucket being monitored.
    - catalog_properties: The properties of the iceberg catalog to connect to.

  Returns:
    bool: True when a table is created, False when it already exists. Is this 
      a good pattern? Who can say 🤷
  """
  catalog = load_catalog('wh', **catalog_properties)
  target_db_name, target_table_name = extract_database_and_table(s3_object_path, s3_monitoring_path)

  # see if the table exists
  try:
    target_table = catalog.load_table(f'{target_db_name}.{target_table_name}')
    logger.info(f"""
    Success - Existing table found in catalog...
      s3_object_path:     {s3_object_path}
      s3_monitoring_path: {s3_monitoring_path}
      target_db_name:     {target_db_name}
      target_table_name:  {target_table_name}
    """)

    return False # if the table exists, we're done here 😎


  except NoSuchTableError as nste:
    # get to boot strappin! 💪
    logger.info(f"""
    Creating table...
      s3_object_path:     {s3_object_path}
      s3_monitoring_path: {s3_monitoring_path}
      target_db_name:     {target_db_name}
      target_table_name:  {target_table_name}
    """)

    s3_uri_file_loader_directory = f's3://{s3_bucket_name}/{s3_monitoring_path}/{target_db_name}/{target_table_name}'
    create_file_loader_target_table(
      s3_uri_file_loader_directory=s3_uri_file_loader_directory, 
      catalog=catalog, 
      database=target_db_name, 
      table=target_table_name
    )

    return True # good work, team 💪

def create_file_loader_target_table(s3_uri_file_loader_directory: str, catalog, database: str, table: str):
    """
    Creates an empty, columnless iceberg table with the given 
    database and table name in the provided iceberg catalog.
    """
    loader_dir = s3_uri_file_loader_directory.strip('/')
    if not loader_dir.startswith('s3://'):
      raise ValueError(f'valid s3 uri must be provided for file loader target table creation. Got: {s3_uri_file_loader_directory}')
    if loader_dir.endswith('.parquet'):
      raise ValueError(f'Expecting an s3 folder path but got: {s3_uri_file_loader_directory}')

    # Create the namespace if it doesn't exist
    try:
      catalog.create_namespace(database)
    except NamespaceAlreadyExistsError as naee:
      pass

    # Create 'db.table'
    table_props = get_tabular_table_properties(loader_dir)
    table_props['comment'] = f'created by cdc bootstrapper to monitor {s3_uri_file_loader_directory}'
    print(table_props)
    catalog.create_table(
      identifier=f'{database}.{table}',
      schema={},
      properties=table_props
    )