import duckdb
import pytest

import batch_bootstrapper

class TestBatchBootstrapper:
  def get_sample_duckdb(self):
    conn = duckdb.connect(database=':memory:', read_only=False)

    mock_data = [
        ('tabular/staged/enterprise_data_warehouse/dir1/dir2/file1',), 
        ('tabular/staged/enterprise_data_warehouse/alpha/beta/file1',), 
        ('tabular/staged/enterprise_data_warehouse/dir3/dir4/',),
        ('tabular/staged/enterprise_data_warehouse/dir3/file5',),
        ('tabular/staged/enterprise_data_warehouse/dir1/dir2/',),
        ('tabular/staged/enterprise_data_warehouse/dir1/',), 
        ('tabular/staged/other_data_dir/dir1/dir2/file2',), 
        ('tabular/staged/enterprise_data_warehouse/dir1/dir2/dir3/file3',),
        ('tabular/staged/enterprise_data_warehouse/bootstrap-raw/',),
        ('tabular/other_root/dir1/dir2/file4',), 
        ('unrelated/dir/dir/file6',)
    ]

    conn.execute("CREATE TABLE s3 (key VARCHAR)")
    conn.executemany("INSERT INTO s3 VALUES (?)", mock_data)
    
    return conn

  def test_get_unique_target_paths(self):
    monitor_path = 'tabular/staged/enterprise_data_warehouse'
    mock_db = self.get_sample_duckdb()
    table_name = 's3'

    expected_targets = {
      'tabular/staged/enterprise_data_warehouse/dir1/dir2',
      'tabular/staged/enterprise_data_warehouse/alpha/beta',
    }

    actual_targets = set(batch_bootstrapper.get_unique_target_paths(mock_db, table_name, monitor_path))
    
    assert expected_targets == actual_targets
