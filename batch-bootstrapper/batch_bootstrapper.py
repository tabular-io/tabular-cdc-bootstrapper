import logging
import os

from pyiceberg.catalog import load_catalog
from pyiceberg import expressions

import tabular

# Configs
TABULAR_CREDENTIAL  = os.environ['TABULAR_CREDENTIAL']
TABULAR_CATALOG_URI = 'https://api.tabular.io/ws'
TABULAR_BASE_URL    = 'https://api.tabular.io'

TABULAR_TARGET_ORG_ID       = '6e332d7a-5325-4d1a-8b13-40181e4158d3'
TABULAR_TARGET_WAREHOUSE    = 'enterprise_data_warehouse'
TABULAR_TARGET_WAREHOUSE_ID = '097a7cbb-5095-40ed-bf80-dabafbacd09e'
TABULAR_TARGET_DATABASE     = 'cdc_bootstrap'
TABULAR_TARGET_DATABASE_ID  = 'f4ec7605-1350-4401-ab12-ca25b3135d50'

TABULAR_CDC_ID_FIELD        = 'id'
TABULAR_CDC_TIMESTAMP_FIELD = 'transact_seq'

# S3 Targets!
S3_URIS = [
  's3://randy-pitcher-workspace--aws--us-west-2/tabular/staged/enterprise_data_warehouse/bootstrap_raw/home_api/',
  's3://randy-pitcher-workspace--aws--us-west-2/tabular/staged/enterprise_data_warehouse/bootstrap_raw/work_api/'
]

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  

def main():
  logging.info(f"""
    Starting batch bootstrapping 💪
      - tabular uri: {TABULAR_CATALOG_URI}
      - tabular target warehouse: {TABULAR_TARGET_WAREHOUSE}
      - tabular target database: {TABULAR_TARGET_DATABASE}
      - s3 targets: {S3_URIS}
  """)
  
  catalog_properties = {
    'uri':        TABULAR_CATALOG_URI,
    'credential': TABULAR_CREDENTIAL,
    'warehouse':  TABULAR_TARGET_WAREHOUSE
  }
  catalog = load_catalog(**catalog_properties)

  for target in S3_URIS:
    try:
      logging.info(f"""
        Processing target: {target}
      """)
      # build table names
      cdc_mirror_table_name = target.strip('/').split('/')[-1]
      changelog_table_name = f'{cdc_mirror_table_name}_changelog'
      
      # Create changelog table
      tabular.bootstrap_table(
        TABULAR_CREDENTIAL,
        TABULAR_BASE_URL,
        TABULAR_TARGET_ORG_ID,
        TABULAR_TARGET_WAREHOUSE_ID,
        TABULAR_TARGET_DATABASE,
        TABULAR_TARGET_DATABASE_ID,
        changelog_table_name,
        target,
        True, # enables file loader
        '**/LOAD*', # excludes initial load files
        catalog
      )
      
      # Create CDC table
      tabular.bootstrap_table(
        TABULAR_CREDENTIAL,
        TABULAR_BASE_URL,
        TABULAR_TARGET_ORG_ID,
        TABULAR_TARGET_WAREHOUSE_ID,
        TABULAR_TARGET_DATABASE,
        TABULAR_TARGET_DATABASE_ID,
        cdc_mirror_table_name,
        target,
        False, # disables automated file loader
        '**/*-*', # excludes DMS changelog files (hopefully)
        catalog
      )
      
      # Configure table properties

    except Exception as exc:
      logging.error(f"""
        Error processing target "{target}"! Will skip it for now 🫠. Actual error below:
      """, exc_info=True)


if __name__ == '__main__':
  main()