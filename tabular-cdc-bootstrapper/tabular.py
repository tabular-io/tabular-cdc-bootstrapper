from pyiceberg.catalog import load_catalog

def extract_database_and_table(s3_key: str, prefix: str):
    """
    Extract the database and table name from the s3 key.
    """
    try:
        paths = s3_key.strip('/').split('/')
        database = paths[0]
        table = paths[1]

        return database, table

    except IndexError:
        raise ValueError("The s3 key must have at least 2 subdirectory levels.")

def bootstrap_from_file(s3_key: str, s3_prefix:str, catalog_properties):
    """
    Create iceberg catalog, check whether the table exists else call the method to create it.
    """
    # TODO: Add logic to build iceberg catalog from catalog properties
    catalog = load_catalog('wh', catalog_properties)
    target_db, target_table = extract_database_and_table(s3_key, s3_prefix)

    table_id = iceberg.TableIdentifier.of(target_database, target_table)
    
    if not catalog.tableExists(table_id):
        # TODO: Fill in create_table_from_s3_path function first
        iceberg_table = create_table_from_s3_path(s3_key, catalog, target_database, target_table)
        
        return iceberg_table
    else:
        raise ValueError("The table already exists.")

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
