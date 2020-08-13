import cx_Oracle
import json


def create_database_connection(settings_file):
    """Connects to an Oracle database, with settings taken from the specified json file

    The json file should specify:
        - host: usually localhost
        - password
        - port: usually 1521
        - service_name
        - user

    Returns a cx_Oracle database connection.
    """
    with open(settings_file, "r") as f:
        db_settings = json.load(f)

    dsn = cx_Oracle.makedsn(
        host=db_settings["host"],
        port=db_settings["port"],
        service_name=db_settings["service_name"],
    )
    connection = cx_Oracle.connect(
        user=db_settings["user"],
        password=db_settings["password"],
        dsn=dsn,
        encoding="UTF-8",
    )
    return connection


def get_table_names(database: str, connection) -> list:
    """Gets names of all tables in the database, skipping any that start with "SYS_"

    Parameters
    ----------
    database (str):
        The name of the database/schema - for example DELIUS_ANALYTICS_PLATFORM

    Returns
    -------
    List of table names as strings

    """
    cursor = connection.cursor()
    cursor.execute(f"SELECT table_name FROM all_tables WHERE OWNER = '{database}'")
    result = cursor.fetchall()
    names = [r[0] for r in result if not r[0].startswith("SYS_")]
    return names


def get_curated_metadata(
    tables: list,
    database: str,
    location="./metadata",
    include_op_column=True,
    include_derived_columns=False,
    connection=None,
):
    """
    Creates a json file of metadata for each table that's named in tables and has rows

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

    connection (cx_Oracle connection):
        Database connection object

    Returns
    -------
    None
        But creates a json file for each table in the database
    """
    cursor = connection.cursor()
    problems = {}
    for table in tables:
        try:
            cursor.execute(f"SELECT count(*) FROM {database}.{table} WHERE ROWNUM <= 1")
            rows = cursor.fetchone()

        except cx_Oracle.DatabaseError as e:
            print(f"Couldn't read {table} in {database} - it might have been deleted")
            problems[table] = e.args[0].message
            continue

        if rows[0] > 0:
            try:
                cursor.execute(f"SELECT * FROM {database}.{table} WHERE ROWNUM <= 1",)

            except cx_Oracle.DatabaseError as e:
                # Catches tables marked for deletion but still in table name list
                print(
                    f"Couldn't select from {table} in {database} - it might have been marked for deletion"
                )
                problems[table] = e.args[0].message
                continue

            metadata = get_table_meta(
                cursor, table, include_op_column, include_derived_columns
            )
            with open(f"{location}/{table.lower()}.json", "w+") as file:
                json.dump(metadata, file, indent=4)
        else:
            print(f"No rows in {table} from {database}")

    if problems:
        print()
        print("ERRORS RAISED")
        for p, e in problems.items():
            print(f"Error in table {p}: {e}")


def get_table_meta(
    cursor, table: str, include_op_column: bool, include_derived_columns: bool
) -> list:
    """
    Lists a table's columns, plus any primary key fields and partitions

    Parameters
    ----------
    cursor:
        The results of a cx_Oracle query to the table - usually
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

    Returns
    -------
    List of dicts
        Contains data for all the columns in the table, ready to write to json
    """
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

    # Main column info - cursor.description has 7 set columns described at:
    # https://cx-oracle.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.description
    for col in cursor.description:
        # skip col types that DMS can't copy
        if col[1].name not in [
            "DB_TYPE_ROWID",
            "DB_TYPE_BFILE",
            "REF",
            "ANYDATA",
        ]:
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
            "description": "",
            "nullable": False,
        },
        {
            "name": "mojap_start_datetime",
            "type": "datetime",
            "description": "",
            "nullable": False,
        },
        {
            "name": "mojap_end_datetime",
            "type": "datetime",
            "description": "",
            "nullable": False,
        },
        {
            "name": "mojap_latest_record",
            "type": "boolean",
            "description": "",
            "nullable": False,
        },
        {
            "name": "mojap_image_tag",
            "type": "character",
            "description": "",
            "nullable": False,
        },
    ]

    if table == "DOCUMENT_HISTORY":
        columns += document_columns

    if include_derived_columns:
        columns += derived_columns

    primary_key_fields = get_primary_key_fields(table=table, cursor=cursor)
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
        "primary_key_fields": primary_key_fields,
    }
    return metadata


def get_primary_key_fields(table, cursor):
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
        primary_key_fields = None
    else:
        primary_key_fields = tuple(item[0].lower() for item in result)
    return primary_key_fields


def get_partitions(table, cursor):
    """Extracts partitions from a table - run as part of get_curated_metadata
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
