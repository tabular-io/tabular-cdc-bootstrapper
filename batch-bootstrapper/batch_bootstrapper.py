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
  # to be eligible to load, a file needs at least 2 levels of directory
  # under the s3_path we're monitoring
  min_eligible_pathcount = len(s3_path.split('/')) + 2

  sql = f"""
    with candidate_paths as (
      select
        split(key, '/') as key_parts,
        key_parts[:{min_eligible_pathcount}] as load_path_parts,
        list_aggregate(load_path_parts, 'string_agg', '/') as target_path

      from
        {duck_table}

      where
        len(key_parts) >= {min_eligible_pathcount}
    )

    select distinct target_path from candidate_paths
  """

  target_paths = [result[0] for result in duck.sql(sql).fetchall()]
  return target_paths


def get_s3_targets_from_tabular(catalog, s3_path):
  existing_tables = get_catalog_contents(catalog)
  existing_load_paths = [f'{s3_path}/{db}/{table}' for db, table in existing_tables]

  s3_inventory_predicates = expressions.And(
    expressions.IsNull('resource_type'), 
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

  catalog = load_catalog(**catalog_properties)

  targets = get_s3_targets_from_tabular(catalog, S3_PATH_TO_MONITOR)

  for target in targets:
    database, table = tabular.extract_database_and_table(target, S3_PATH_TO_MONITOR, is_dir=True)
    tabular.create_file_loader_target_table(f's3://{target}', catalog, database, table)
    print(target)


if __name__ == '__main__':
    main()