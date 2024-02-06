import logging

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import StringType, TimestampType, NestedField

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_and_table_from_s3_path(file_loader_s3_path: str):
  """
  Extract the database and table name from the s3 object path

  Args:
    - file_loader_s3_path: str - folder path that should be monitored by file loader. 

  Returns:
    (database_name, table_name): tuple(str, str) - db name and table name strings from the given path
  """
  logger.info(f"""
    Extracting database and table name from:
      - file_loader_s3_path: "{file_loader_s3_path}"
  """)
  
  try:
    database_name, table_name = file_loader_s3_path.strip('/').split('/')[-2:]
    return (database_name, table_name)

  except IndexError:
    raise ValueError(f"""
      The file_loader_s3_path must have at least 2 directory levels, but got {file_loader_s3_path}
    """)

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
  generates the appropriate tabular properties dictionary for an iceberg table requiring file loading
  and cdc processing. 

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

def bootstrap_cdc_target(
  s3_file_loader_target_path: str, 
  s3_bucket_name: str,  
  cdc_id_field: str,
  cdc_timestamp_field: str,
  catalog: Catalog
  ) -> str:
  """
  Connects to an iceberg rest catalog with catalog_properties and bootstraps
  a new table if one doesn't already exist

  Args:
    - s3_file_loader_target_path (str): The S3 object path to the file_loader 
      directory path to process.
    - s3_bucket_name (str): The name of the S3 bucket being monitored.
    - cdc_id_field (str): The name of the field in the file loader target to use for cdc ids.
    - cdc_timestamp_field (str): The name of the field in the file loader target to use for cdc timestamps .
    - catalog (Catalog): Pyiceberg catalog to use for querying

  Returns:
    bool: True when a table is created, False when it already exists. Is this 
      a good pattern? Who can say 🤷
  """
  target_db_name, target_table_name = get_db_and_table_from_s3_path(s3_file_loader_target_path)

  # see if the table exists
  try:
    target_table = catalog.load_table(f'{target_db_name}.{target_table_name}')
    logger.info(f"""
    Success - Existing table found in catalog...
      s3_file_loader_target_path: {s3_file_loader_target_path}
      target_db_name:             {target_db_name}
      target_table_name:          {target_table_name}
    """)

    return False # if the table exists, we're done here 😎


  except NoSuchTableError as nste:
    # get to boot strappin! 💪
    logger.info(f"""
      Creating table...
        s3_file_loader_target_path: {s3_file_loader_target_path}
        target_db_name:             {target_db_name}
        target_table_name:          {target_table_name}
    """)

    s3_file_loader_target_uri = f's3://{s3_bucket_name}/{s3_file_loader_target_path}'
    file_loader_target_table = create_file_loader_target_table(
      s3_uri_file_loader_directory=s3_file_loader_target_uri, 
      catalog=catalog, 
      database=target_db_name, 
      table=target_table_name
    )

    logger.info(f"""
      Successfully created file loader target table.

      Now creating cdc target table:
        - cdc_id_field        = "{cdc_id_field}"
        - cdc_timestamp_field = "{cdc_timestamp_field}"
    """)

    create_cdc_target_table(
      cdc_source_table=file_loader_target_table, 
      cdc_id_field=cdc_id_field,
      cdc_timestamp_field=cdc_timestamp_field,
      catalog=catalog
    )

    return True # good work, team 💪

def create_file_loader_target_table(
  s3_uri_file_loader_directory: str, 
  catalog: Catalog, 
  database: str, 
  table: str
  ):
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
  table_props = get_file_loader_target_table_properties(loader_dir)
  table_props['comment'] = f'created by cdc bootstrapper to monitor {s3_uri_file_loader_directory}'

  return catalog.create_table(
    identifier=f'{database}.{table}',
    schema={},
    properties=table_props
  )

def create_cdc_target_table(
  cdc_source_table: Table, 
  cdc_id_field: str, 
  cdc_timestamp_field: str, 
  catalog: Catalog
  ):
  """
  Creates a cdc target iceberg table in the same namespace as the given 
  source table. It will be named the same as the source table but 
  with a _cdc_target postfix

  Args:
    - cdc_source_table: Table -- pyiceberg table to use as a cdc source
    - catalog: Catalog -- pyiceberg catalog to create the cdc target in
  """
  # Create table
  _, source_db, source_table = cdc_source_table.name()
  target_table_identifier = f'{source_db}.{source_table}__cdc_target'
  table_props = get_cdc_target_table_properties(cdc_id_field=cdc_id_field, cdc_timestamp_field=cdc_timestamp_field)
  table_props['comment'] = f'created by cdc bootstrapper to perform cdc mirroring of {source_db}.{source_table}'

  catalog.create_table(
    identifier=target_table_identifier,
    schema= Schema(
      NestedField(field_id=1, name=cdc_id_field, field_type=StringType(), required=True),
      NestedField(field_id=2, name=cdc_timestamp_field, field_type=TimestampType(), required=True),
    ),
    properties=table_props
  )

  # now update the cdc source table with a cdc property
  with cdc_source_table.transaction() as txn:
    txn.set_properties(**{'dependent-tables': target_table_identifier})