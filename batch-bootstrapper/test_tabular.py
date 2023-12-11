import os

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
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


  def test_get_db_and_table_from_s3_path(self):
    expected_db    = 'alpha'
    expected_table = 'beta'
    target         = f'cdc-bootstrap/{expected_db}/{expected_table}'
    postfixes      = ['', '/'] # check that trailing '/'s are tolerated well

    for postfix in postfixes:
      actual_db, actual_table = tabular.get_db_and_table_from_s3_path(target + postfix)
      assert expected_db == actual_db
      assert expected_table == actual_table
    
    with pytest.raises(ValueError):
      tabular.get_db_and_table_from_s3_path('asdfasdfasdf')

  def test_bootstrap_cdc_target(self):
    target_db    = '_cdc_bootstrapper_ci'
    target_table = 'missing-table'
    test_cases   = {
      'table_exists':     ('cdc-bootstrap/system/catalog_events', False),
      'table_missing':    (f'cdc-bootstrap/{target_db}/{target_table}', True),
    }

    try:
      for key in test_cases:
        test_case = test_cases[key]
        expected  = test_case[-1]
        actual    = tabular.bootstrap_cdc_target(
          test_case[0], 
          self.S3_BUCKET_TO_MONITOR, 
          self.catalog
        )
        assert actual == expected
      
      # test some junk
      with pytest.raises(ValueError):
        tabular.bootstrap_cdc_target('lkdfj.jdsfskl', 'fdassdf', self.catalog)

    finally:
      try:
        self.catalog.drop_table(f'{target_db}.{target_table}')
        self.catalog.drop_namespace(target_db)
      except NoSuchTableError as nste:
        pass # if the test fails before a table is created, this error can happen. ndb ⚡


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
