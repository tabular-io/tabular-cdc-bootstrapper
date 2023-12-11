from io import BytesIO
import os

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
import pytest

import tabular

class TestTabular:
  # get env vars
  S3_BUCKET_TO_MONITOR        = os.environ['S3_BUCKET_TO_MONITOR']
  S3_PATH_TO_MONITOR          = os.environ['S3_PATH_TO_MONITOR']
  TABULAR_CDC_ID_FIELD        = os.environ['TABULAR_CDC_ID_FIELD']
  TABULAR_CDC_TIMESTAMP_FIELD = os.environ['TABULAR_CDC_TIMESTAMP_FIELD']

  CATALOG_PROPERTIES = {
    'type':       'rest',
    'uri':        os.environ['TABULAR_CATALOG_URI'],
    'credential': os.environ['TABULAR_CREDENTIAL'],
    'warehouse':  os.environ['TABULAR_TARGET_WAREHOUSE'],
  }
  catalog = load_catalog(**CATALOG_PROPERTIES)


  def test_extract_database_and_table(self):
    s3_key = 'cdc-bootstrap/alpha/gazebo/my-file.json'

    database, table = tabular.extract_database_and_table(s3_key)
    assert database == 'cdc-bootstrap' and table == 'alpha'

    database, table = tabular.extract_database_and_table(s3_key, '')
    assert database == 'cdc-bootstrap' and table == 'alpha'

    database, table = tabular.extract_database_and_table(s3_key, 'cdc-bootstrap')
    assert database == 'alpha' and table == 'gazebo'
    
    with pytest.raises(ValueError):
      tabular.extract_database_and_table(s3_key, 'cdc-bootstrap/alpha')

  def test_bootstrap_cdc_target(self):
    target_db    = '_cdc_bootstrapper_ci'
    target_table = 'missing-table'
    test_cases   = {
      'table_exists':     ('cdc-bootstrap/system/catalog_events/my-file.json', 'cdc-bootstrap', False),
      'table_missing':    (f'cdc-bootstrap/{target_db}/{target_table}/my-file.json', 'cdc-bootstrap', True),
    }

    try:
      for key in test_cases:
        test_case = test_cases[key]
        expected  = test_case[2]
        actual    = tabular.bootstrap_cdc_target(
          test_case[0], 
          test_case[1], 
          self.S3_BUCKET_TO_MONITOR, 
          self.CATALOG_PROPERTIES
        )
        assert actual == expected
      
      # test some junk
      with pytest.raises(ValueError):
        tabular.bootstrap_cdc_target('lkdfj.jdsfskl', 'fdassdf', 'dklfjasldkf', {})

    finally:
      self.catalog.drop_table(f'{target_db}.{target_table}')
      self.catalog.drop_namespace(target_db)


  def test_create_file_loader_target_table(self):
    target_db_name = '_test_cdc_bootloader'
    target_table_name = '_test_create_table_from_s3_path'
    s3_uri_file_loader_directory = f's3://{self.S3_BUCKET_TO_MONITOR}/{self.S3_PATH_TO_MONITOR}/{target_db_name}/{target_table_name}'

    try:
      tabular.create_file_loader_target_table(
        s3_uri_file_loader_directory=s3_uri_file_loader_directory, 
        catalog=self.catalog, 
        database=target_db_name, 
        table=target_table_name
      )

      actual_table        = self.catalog.load_table(f'{target_db_name}.{target_table_name}')
      expected_table_name = ('default', target_db_name, target_table_name)

      assert actual_table.name() == expected_table_name
    
    finally:
      self.catalog.drop_table(f'{target_db_name}.{target_table_name}')
      self.catalog.drop_namespace(target_db_name)
