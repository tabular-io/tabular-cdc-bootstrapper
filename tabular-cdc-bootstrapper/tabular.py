from io import BytesIO
import logging

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_s3_monitoring_uri(s3_monitoring_uri: str) -> (str, str):
  """
  Returns (bucket_name, monitoring_path) from the s3 uri to monitor for bootstrapping
  """
  if not s3_monitoring_uri or not s3_monitoring_uri.startswith('s3://'):
    raise ValueError(f'Valid s3_monitoring_uri values must start with "s3://". Instead got "{s3_monitoring_uri}"')

  uri_components = s3_monitoring_uri[len('s3://'):].split('/')
  if [''] == uri_components:
    raise ValueError(f'No bucket name found in s3_monitoring_uri - got "{s3_monitoring_uri}"')

  bucket_name     = uri_components[0]
  monitoring_path = '/'.join(uri_components[1:])
  return (bucket_name, monitoring_path)

def extract_database_and_table(s3_object_path: str, s3_monitoring_path: str = ''):
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

  try:
    relevant_parent_folders = relevant_s3_path.split('/')[:-1] #ignore the actual file by dropping the last element (-1 index)
    database = relevant_parent_folders[0]
    table = relevant_parent_folders[1]

    return database, table

  except IndexError:
    raise ValueError("The s3 key must have at least 2 subdirectory levels.")

def get_table_schema_from_parquet(parquet_io_object: BytesIO) -> dict: 
  # read that schema
  parquet_io_object.seek(0)
  table = pq.read_table(source=parquet_io_object)
  return table.schema

def get_tabular_table_properties(file_loader_path: str, cdc_id_field: str = '', cdc_timestamp_field: str = '') -> dict:
  """
  generates the appropriate tabular properties dictionary for an iceberg table requiring file loading
  and cdc processing. 

  Args:
    - file_loader_path (str): s3 uri that should be monitored for new files to load.
        For example: s3://randy-pitcher-workspace--aws/cdc-bootstrap/alpha/pyiceberg/some_data.parquet

    - cdc_id_field (str): column in the table representing the unique identity of each row in the cdc output. Often an id.
        For example: 'customer_id'. This tells tabular whether to update or insert a row.

    - cdc_timestamp_field (str): column in the table representing the timestamp in the current timestamp to use to determine
        which records belong to different points in time, specifically which records are the latest.
        For example: 'last_updated_at'. 
  """
  properties = {}

  if file_loader_path:
    # https://docs.tabular.io/tables#file-loader-properties
    properties['fileloader.enabled']       = 'true'
    properties['fileloader.path']          = file_loader_path
    properties['fileloader.file-format']   = 'parquet'
    properties['fileloader.write-mode']    = 'append'
    properties['fileloader.evolve-schema'] = 'true'

  return properties

def bootstrap_from_file(s3_object_path: str, s3_monitoring_uri:str, catalog_properties) -> str:
  """
  Connects to an iceberg rest catalog with catalog_properties and bootstraps 
  a new table if one doesn't already exist
  
  Returns:
    bool: True when a table is created, False when it already exists. Is this a good pattern? Who can say.
  """
  catalog = load_catalog('wh', **catalog_properties)
  bucket_name, monitoring_path = parse_s3_monitoring_uri(s3_monitoring_uri)
  target_db_name, target_table_name = extract_database_and_table(s3_object_path, monitoring_path)

  # see if the table exists
  try:
    target_table = catalog.load_table(f'{target_db_name}.{target_table_name}')
    logger.info(f"""
    Success - Existing table found in catalog...
      s3_object_path:    {s3_object_path}
      s3_monitoring_uri: {s3_monitoring_uri}
      target_db_name:    {target_db_name}
      target_table_name: {target_table_name}
    """)

    return False # if the table exists, we're done here 😎


  except NoSuchTableError as nste:
    # get to boot strappin! 💪
    logger.info(f"""
    Creating table...
      s3_object_path:    {s3_object_path}
      s3_monitoring_uri: {s3_monitoring_uri}
      target_db_name:    {target_db_name}
      target_table_name: {target_table_name}
    """)

    file_loader_monitoring_path = f'{s3_monitoring_uri}/{target_db_name}/{target_table_name}'
    create_file_loader_target(
      file_loader_monitoring_path, 
      catalog=catalog, 
      database=target_db_name, 
      table=target_table_name
    )

    return True # good work, team 💪

def create_file_loader_target(s3_uri_file_loader_directory: str, catalog, database: str, table: str):
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

# todo: create_cdc_mirror_target