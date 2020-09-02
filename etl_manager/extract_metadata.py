import os

from etl_manager.utils import write_json


def get_table_names(database: str, connection, include_tablespace=False) -> list:
    """Gets names of tables in the database

    You can save the output into a file, or pass it directly to create_table_json

    Parameters
    ----------
    database (str):
        The name of the database/schema - for example DELIUS_ANALYTICS_PLATFORM

    connection (database connection object):
        The database connection to query - for example a cx_Oracle.connect() object

    include_tablespace (bool):
        If False, return list of table names.
        If true, return list of tuples of table name and tablespace

    Returns
    -------
    List of table names as strings.
    If including tablespaces, returns list of tuples of table names and tablespaces

    """
    cursor = connection.cursor()
    cursor.execute(f"SELECT table_name, tablespace_name FROM all_tables WHERE OWNER = '{database}'")
    result = cursor.fetchall()
    if include_tablespace:
        return [(r[0], r[1]) for r in result]
    else:
        return [r[0] for r in result]


def create_json_for_database(
    description: str, name: str, bucket: str, base_folder: str, location="./meta_data",
):
    """Creates a database.json file suitable for reading with read_database_folder()

    Parameters
    ----------
    description (str):
        Text saying where the data comes from

    name (str):
        What you want the database to be called in Glue and Athena. Use underscores

    bucket (str):
        The S3 bucket where the actual data will end up

    base_folder (str):
        The folder/prefix within the S3 bucket for the data you want
        Use capitals if the S3 location includes capitals
        Don't go down to the table level - it should be the level above
        For example "hmpps/delius/DELIUS_ANALYTICS_PLATFORM"

    location (str):
        Folder where you'd like to create the json metadata

    Returns
    -------
    None, but creates a json file in the specified location.
    """

    db = {
        "description": description,
        "name": name,
        "bucket": bucket,
        "base_folder": base_folder,
    }
    write_json(db, os.path.join(location, "database.json"))


def create_json_for_tables(
    tables: list,
    database: str,
    location="./meta_data",
    include_op_column=True,
    include_derived_columns=False,
    include_objects=False,
    connection=None,
):
    """
    Creates a json file of metadata for each table that's named in tables and has rows
    These json files are suitable for reading with read_database_folder()

    Parameters
    ----------
    tables (list):
        A list of table names from the source database

    database (str):
        The name of the database

    location (string):
        Folder where you want to save the metadata

    include_op_column (boolean):
        If True, adds a column called 'Op' for operation type - I, U or D
        This data is automatically added by AWS DMS ongoing replication tasks

    include_derived_columns (boolean):
        If True, adds 5 datetime columns: mojap_extraction_datetime,
        mojap_start_datetime, mojap_end_datetime, mojap_latest_record, mojap_image_tag

    connection (database connection object):
        The database connection to query - for example a cx_Oracle.connect() object

    Returns
    -------
    None
        But creates a json file for each table in the database
    """
    cursor = connection.cursor()
    problems = {}
    for table in tables:
        try:
            # Get a row to see the columns and check the table has data
            # 'WHERE ROWNUM <= 1' is Oracle for 'LIMIT 1'
            # fetchone() to see the first row of the query executed
            cursor.execute(f"SELECT * FROM {database}.{table} WHERE ROWNUM <= 1")
            cursor.fetchone()
            # For Delius purposes we only want tables with at least 1 row
            # This might not be the case for all metadata
            if cursor.rowcount > 0:
                metadata = get_table_meta(
                    cursor, table, include_op_column, include_derived_columns
                )
                write_json(metadata, os.path.join(location, f"{table.lower()}.json"))

            else:
                print(f"No rows in {table} from {database}")

        except Exception as e:
            # Likely errors are that the table has been deleted, marked
            # for deletion, or relies on an external reference you can't access
            print(f"Problem reading {table} in {database}")
            problems[table] = e.args[0].message  # this attribute may be Oracle-only
            continue

    # Print the error messages at the end
    if problems:
        print()
        print("ERRORS RAISED")
        for p, e in problems.items():
            print(f"Error in table {p}: {e}")

    cursor.close()
    connection.close()


def get_table_meta(
    cursor,
    table: str,
    include_op_column: bool = True,
    include_derived_columns: bool = False,
    include_objects: bool = False,
) -> list:
    """
    Lists a table's columns, plus any primary key fields and partitions

    Parameters
    ----------
    cursor:
        A cursor where .execute has already been run - usually querying
        "SELECT * FROM {database}.{table} WHERE ROWNUM <= 1".
        This will give the cursor a .description attribute with column info

    table (str):
        Name of the table

    include_op_column (boolean):
        If True, adds a column called 'Op' for operation type - I, U or D
        This data is automatically added by AWS DMS ongoing replication tasks

    include_derived_columns (boolean):
        If True, adds 5 datetime columns: mojap_extraction_datetime,
        mojap_start_datetime, mojap_end_datetime, mojap_latest_record, mojap_image_tag

    include_objects (boolean):
        If True, will include metadata for DB_TYPE_OBJECT columns as array<character>.
        Leave False to ignore these columns - AWS DMS doesn't extract them.

    Returns
    -------
    List of dicts
        Contains data for all the columns in the table, ready to write to json
    """
    # This lookup is specific to Oracle data types
    # Likely to need separate dictionaries for other data sources
    type_lookup = {
        "DB_TYPE_DATE": "datetime",
        "DB_TYPE_TIMESTAMP": "datetime",
        "DB_TYPE_TIMESTAMP_TZ": "datetime",
        "DB_TYPE_CHAR": "character",
        "DB_TYPE_CLOB": "character",
        "DB_TYPE_BLOB": "character",
        "DB_TYPE_VARCHAR": "character",
        "DB_TYPE_LONG": "character",
        "DB_TYPE_RAW": "character",
        "DB_TYPE_OBJECT": "array<character>",
    }
    columns = []

    if include_op_column:
        columns.append(
            {
                "name": "op",
                "type": "character",
                "description": "Type of change, for rows added by ongoing replication.",
                "nullable": True,
                "enum": ["I", "U", "D"],
            }
        )

    # Data types to skip. This list is specific to working with
    # Amazon Database Migration Service and an Oracle database.
    # These are the Oracle datatypes that DMS can't copy
    if include_objects:
        skip = [
            "DB_TYPE_ROWID",
            "DB_TYPE_BFILE",
            "REF",
            "ANYDATA",
        ]
    else:
        skip = [
            "DB_TYPE_ROWID",
            "DB_TYPE_BFILE",
            "REF",
            "ANYDATA",
            "DB_TYPE_OBJECT",
        ]
    # Main column info - cursor.description has 7 set columns:
    # name, type, display_size, internal_size, precision, scale, null_ok
    # Usually type is just a lookup, but decimal types need scale and precision too
    for col in cursor.description:
        if col[1].name not in skip:
            columns.append(
                {
                    "name": col[0].lower(),
                    "type": f"decimal({col[4]},{col[5]})"
                    if col[1].name == "DB_TYPE_NUMBER"
                    else type_lookup[col[1].name],
                    "description": "",
                    "nullable": bool(col[6]),
                }
            )

    document_columns = [
        {
            "name": "mojap_document_path",
            "type": "character",
            "description": "The path to the document",
            "nullable": True,
        }
    ]
    derived_columns = [
        {
            "name": "mojap_extraction_datetime",
            "type": "datetime",
            "description": "When this data was extracted from its source database",
            "nullable": False,
        },
        {
            "name": "mojap_start_datetime",
            "type": "datetime",
            "description": "When this record started to be the current data for this primary key",
            "nullable": False,
        },
        {
            "name": "mojap_end_datetime",
            "type": "datetime",
            "description": "When this record stopped being the current data for this primary key",
            "nullable": False,
        },
        {
            "name": "mojap_latest_record",
            "type": "boolean",
            "description": "Whether this record is currently the latest one for this primary key",
            "nullable": False,
        },
        {
            "name": "mojap_image_tag",
            "type": "character",
            "description": "A tag of the docker image used to run the pipeline tasks, if relevant",
            "nullable": False,
        },
    ]

    # Not yet used - retained from Oasys metadata functions in case it helps later
    if table == "DOCUMENT_HISTORY":
        columns += document_columns

    if include_derived_columns:
        columns += derived_columns

    primary_keys = get_primary_keys(table=table, cursor=cursor)
    partitions = get_partitions(table=table, cursor=cursor)

    metadata = {
        "$schema": (
            "https://moj-analytical-services.github.io/metadata_schema/table/"
            "v1.1.0.json"
        ),
        "name": table.lower(),
        "description": "",
        "data_format": "parquet",
        "columns": columns,
        "location": f"{table}/",
        "partitions": partitions,
        "primary_key": primary_keys,
    }
    return metadata


def get_primary_keys(table, cursor):
    """Looks through constraints for primary keys, and checks they match colums
    Run as part of get_curated_metadata
    """
    statement = (
        "SELECT cols.column_name "
        "FROM all_constraints cons, all_cons_columns cols "
        "WHERE cons.constraint_type = 'P' "
        "AND cons.constraint_name = cols.constraint_name "
        "AND cons.owner = cols.owner "
        "AND cons.status = 'ENABLED' "
        "AND cons.table_name = :table_name "
        "ORDER BY cols.table_name, cols.position"
    )
    cursor.execute(statement, table_name=table)
    result = cursor.fetchall()
    if result == []:
        print(f"No primary key fields found for table {table.lower()}")
        primary_keys = None
    else:
        primary_keys = [item[0].lower() for item in result]
    return primary_keys


def get_partitions(table, cursor):
    """Extracts partitions and their subpartitions from a table

    Run as part of get_curated_metadata
    """
    statement = (
        "SELECT partition_name "
        "FROM ALL_TAB_PARTITIONS "
        "WHERE table_name = :table_name "
        "ORDER BY partition_name"
    )
    cursor.execute(statement, table_name=table)
    result = cursor.fetchall()
    if result == []:
        print(f"No partitions found for table {table.lower()}")
        return None
    else:
        partitions = [item[0] for item in result]
        output = []
        for partition in partitions:
            subpartitions = get_subpartitions(
                table=table, partition=partition, cursor=cursor
            )
            if subpartitions == []:
                print(
                    f"No subpartitions found for partition {partition.lower()} "
                    f"in table {table.lower()}"
                )
                partition_dict = {"name": partition, "subpartitions": None}
            else:
                partition_dict = {"name": partition, "subpartitions": subpartitions}
            output.append(partition_dict)
        return output


def get_subpartitions(table, partition, cursor):
    """Extracts subpartitions - run as part of get_partitions
    """
    statement = (
        "SELECT subpartition_name "
        "FROM ALL_TAB_SUBPARTITIONS "
        "WHERE table_name = :table_name "
        "AND partition_name = :partition_name "
        "ORDER BY subpartition_name"
    )
    cursor.execute(statement, table_name=table, partition_name=partition)
    result = cursor.fetchall()
    subpartitions = [item[0] for item in result]
    return subpartitions
