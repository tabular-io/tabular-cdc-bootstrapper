from io import BytesIO
import os

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
import pytest

import tabular

class TestTabular:
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
    
    with pytest.raises(ValueError):
      tabular.extract_database_and_table(s3_key, 'non/overlapping/monitoring/path')

  def test_bootstrap_from_file(self):
    test_cases = {
      'table_exists':     ('cdc-bootstrap/system/catalog_events/my-file.json', 's3://some_bucket/cdc-bootstrap', False),
      'table_missing':    ('cdc-bootstrap/_test_cdc_bootloader/missing-table/my-file.json', 's3://some_bucket/cdc-bootstrap', True),
    }

    try:
      for key in test_cases:
        test_case = test_cases[key]
        assert tabular.bootstrap_from_file(test_case[0], test_case[1], self.CATALOG_PROPERTIES) == test_case[2]
      
      # test some junk
      with pytest.raises(ValueError):
        tabular.bootstrap_from_file('lkdfj.jdsfskl', 'fdassdf', {})
    
    # cleanup the missing table or future tests will always fail
    finally:
      target_db_name, target_table_name = test_cases['table_missing'][0].split('/')[1:-1]
      self.catalog.drop_table(f'{target_db_name}.{target_table_name}')
      self.catalog.drop_namespace(target_db_name)

  def test_get_table_schema_from_parquet(self):
    test_sets = [
      {
        'Name': ['John', 'Anna', 'Peter', 'Linda'],
        'Age': [30, 20, 40, 50]
      },
    ]

    for test_set in test_sets:
      # turn the test set into an in-memory parquet file
      tbl = pa.table(test_set)
      parquet_file = BytesIO()
      pq.write_table(tbl, parquet_file)

      # get the actual schema 
      actual_schema = tabular.get_table_schema_from_parquet(parquet_file)

      # get expected values
      expected_field_names = set(test_set.keys())

      # assert 💪
      assert set(actual_schema.names) == expected_field_names

  def test_create_file_loader_target(self):
    target_db_name = '_test_cdc_bootloader'
    target_table_name = '_test_create_table_from_s3_path'
    mock_s3_target_uri = f's3://some-bucket/cdc-bootstrap/{target_db_name}/{target_table_name}'

    try:
      tabular.create_file_loader_target(mock_s3_target_uri, catalog=self.catalog, database=target_db_name, table=target_table_name)
      actual_table = self.catalog.load_table(f'{target_db_name}.{target_table_name}')

      expected_table_name = ('default', target_db_name, target_table_name)

      assert actual_table.name() == expected_table_name
    
    finally:
      self.catalog.drop_table(f'{target_db_name}.{target_table_name}')
      self.catalog.drop_namespace(target_db_name)

  def test_parse_s3_monitoring_uri(self):
    test_cases = [
      ('s3://some_bucket', ('some_bucket', '')),
      ('s3://some_bucket/some_path', ('some_bucket', 'some_path')),
      ('s3://some_bucket/some/nested/path', ('some_bucket', 'some/nested/path'))
    ]

    for tc in test_cases:
      assert tc[1] == tabular.parse_s3_monitoring_uri(tc[0])
    
    with pytest.raises(ValueError):
      tabular.parse_s3_monitoring_uri('feefasdfasdflkjh') 
    with pytest.raises(ValueError):
      tabular.parse_s3_monitoring_uri('s3://') 
    with pytest.raises(ValueError):
      tabular.parse_s3_monitoring_uri('') 
