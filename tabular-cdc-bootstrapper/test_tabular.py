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