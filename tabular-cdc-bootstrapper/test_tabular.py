import pytest

import tabular

class TestTabular:
  def test_extract_database_and_table(self):
    s3_key = 'cdc-bootstrap/alpha/my-file.json'
    
    with pytest.raises(ValueError):
      tabular.extract_database_and_table(s3_key, 'cdc-bootstrap')