from io import BytesIO
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import tabular

class TestTabular:
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

  def test_bootstrap_from_file(self):
    test_cases = {
      'table_exists':     ('cdc-bootstrap/system/catalog_events/my-file.json', 'cdc-bootstrap', False),
      'table_missing':    ('cdc-bootstrap/fingers-crossed-this-doesnt-exist/missing-table/my-file.json', 'cdc-bootstrap', True),
      'database_missing': ('cdc-bootstrap/pyiceberg/alpha/my-file.json', '', True)
    }

    catalog_properties = {
      'type':       'rest',
      'uri':        os.environ['TABULAR_CATALOG_URI'],
      'credential': os.environ['TABULAR_CREDENTIAL'],
      'warehouse':  os.environ['TABULAR_TARGET_WAREHOUSE'],
    }

    for key in test_cases:
      test_case = test_cases[key]
      assert tabular.bootstrap_from_file(test_case[0], test_case[1], catalog_properties) == test_case[2]
    
    # test some junk
    with pytest.raises(ValueError):
      tabular.bootstrap_from_file('lkdfj.jdsfskl', 'fdassdf', {})

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