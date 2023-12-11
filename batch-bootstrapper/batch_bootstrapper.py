import logging
import os

from pyiceberg.catalog import load_catalog
from pyiceberg import expressions

import tabular

# Tabular connectivity
TABULAR_CREDENTIAL       = os.environ['TABULAR_CREDENTIAL']
TABULAR_CATALOG_URI      = os.environ['TABULAR_CATALOG_URI']
TABULAR_TARGET_WAREHOUSE = os.environ['TABULAR_TARGET_WAREHOUSE']

# cdc configs
TABULAR_CDC_ID_FIELD        = os.environ['TABULAR_CDC_ID_FIELD']
TABULAR_CDC_TIMESTAMP_FIELD = os.environ['TABULAR_CDC_TIMESTAMP_FIELD']

# S3 Monitoring
S3_BUCKET_TO_MONITOR = os.environ['S3_BUCKET_TO_MONITOR']
S3_PATH_TO_MONITOR   = os.environ['S3_PATH_TO_MONITOR']

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_catalog_contents(catalog):
  databases = [ns[0] for ns in catalog.list_namespaces()]
  databases.remove('system')
  databases.remove('examples')
  
  # python sum of a list of list just concats all the lists
  # but it needs an empty list at the end of the call to work
  tables = sum([catalog.list_tables(db) for db in databases], [])
  
  return tables


def get_unique_target_paths(duck, duck_table, s3_path):
  path_to_monitor = s3_path if s3_path.endswith('/') else s3_path + '/'
  
  sql = f"""
    select distinct
      array_to_string(
        regexp_extract_all(
          key, 
          '{path_to_monitor}[^/]*/[^/]*'
        ), 
        '/'
      ) as target_paths

    from
      {duck_table}

    where
      key like '{path_to_monitor}%/%/_%';
  """

  target_paths = [result[0] for result in duck.sql(sql).fetchall()]
  return target_paths


def get_s3_targets_from_tabular(catalog, s3_bucket, s3_path):
  existing_tables = get_catalog_contents(catalog)
  existing_load_paths = [f'{s3_path}/{db}/{table}' for db, table in existing_tables]

  s3_inventory_predicates = expressions.And(
    expressions.IsNull('resource_type'), 
    expressions.StartsWith('bucket', s3_bucket),
    expressions.StartsWith('key', s3_path)
  )

  # let's build a WILD predicate set 
  for elp in existing_load_paths:
    s3_inventory_predicates = expressions.And(
      s3_inventory_predicates, 
      expressions.NotStartsWith('key', elp)
    )

  sys_table = catalog.load_table('system.s3_inventory_list')
  loadable_s3_keys = sys_table.scan(
    row_filter=s3_inventory_predicates
  )

  duck = loadable_s3_keys.to_duckdb('s3')
  target_paths = get_unique_target_paths(duck, 's3', s3_path)

  return target_paths
  

def main():
  catalog_properties = {
    'uri':        TABULAR_CATALOG_URI,
    'credential': TABULAR_CREDENTIAL,
    'warehouse':  TABULAR_TARGET_WAREHOUSE
  }

  logging.info(f"""
    Starting batch bootstrapping 💪
      - tabular uri: {TABULAR_CATALOG_URI}
      - tabular target warehouse: {TABULAR_TARGET_WAREHOUSE}
      - s3 bucket to monitor: {S3_BUCKET_TO_MONITOR}
      - s3 path to monitor: {S3_PATH_TO_MONITOR}
  """)

  catalog = load_catalog(**catalog_properties)

  targets = get_s3_targets_from_tabular(catalog, S3_BUCKET_TO_MONITOR, S3_PATH_TO_MONITOR)

  if not targets:
    logging.info('No targets to process. That was easy 💃')

  for target in targets:
    try:
      logging.info(f"""
        Processing target: {target}
      """)
      tabular.bootstrap_cdc_target(
        s3_file_loader_target_path=target, 
        s3_bucket_name=S3_BUCKET_TO_MONITOR,
        cdc_id_field=TABULAR_CDC_ID_FIELD,
        cdc_timestamp_field=TABULAR_CDC_TIMESTAMP_FIELD,
        catalog=catalog
      )

    except Exception as exc:
      logging.error(f"""
        Error processing target "{target}"! Will skip it for now 🫠. Actual error below:
      """, exc_info=True)


if __name__ == '__main__':
  main()