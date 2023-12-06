import logging
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_database_and_table(s3_key: str, prefix: str = ''):
    """
    Extract the database and table name from the s3 key.
    """
    relevant_s3_path = s3_key[len(prefix):].strip('/')

    try:
        relevant_parent_folders = relevant_s3_path.split('/')[:-1] #ignore the actual file by dropping the last element (-1 index)
        database = relevant_parent_folders[0]
        table = relevant_parent_folders[1]

        return database, table

    except IndexError:
        raise ValueError("The s3 key must have at least 2 subdirectory levels.")


def bootstrap_from_file(s3_key: str, s3_prefix:str, catalog_properties) -> str:
    """
    Connects to an iceberg rest catalog with properties and bootstraps a new table if one doesn't already exist
    
    Returns:
        bool: True when a table is created, False when it already exists. Is this a good pattern? Who can say.
    """
    catalog = load_catalog('wh', **catalog_properties)
    target_db_name, target_table_name = extract_database_and_table(s3_key, s3_prefix)

    # see if the table exists
    try:
      target_table = catalog.load_table(f'{target_db_name}.{target_table_name}')
      logger.info(f"""Success - Existing table found in catalog...
        s3_key: {s3_key}
        s3_prefix: {s3_prefix}
        target_db_name: {target_db_name}
        target_table_name: {target_table_name}

      """)

      return False # if the table exists, we're done here 😎

    except NoSuchTableError as nste:
      # get to boot strappin! 💪
      logger.info(f"""Success - No table found in catalog, yet...
        s3_key: {s3_key}
        s3_prefix: {s3_prefix}
        target_db_name: {target_db_name}
        target_table_name: {target_table_name}

      """)

      return True


def create_table_from_s3_path(s3_key: str, catalog, database: str, table: str):
    """
    Call tabular API to infer the schema of the table to be created from the s3 path.
    Then create the table.
    """
    # TODO: connection to the placeholder tabular API
    # Use the api to infer schema from s3 path
    # schema = infer_schema(tabular_api, s3_key)

    # Once we have the schema
    # Create a table using the catalog object
    # table_id = iceberg.TableIdentifier.of(database, table)
    # iceberg_table = catalog.createTable(table_id, schema)

    # return iceberg_table
    raise NotImplementedError

def bootstrap_load_table(s3_folder_path: str, warehouse: str, database: str, table: str):
    """
    Call tabular API to load data from the folder into the table
    """
    # TODO: Call the tabular API to load data
    raise NotImplementedError
