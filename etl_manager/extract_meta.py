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
        user=db_settings["user"], password=db_settings["password"], dsn=dsn
    )
    return connection


def get_curated_metadata(
    tables: list,
    database: str,
    location="./metadata",
    include_op_column=True,
    include_derived_columns=False,
    connection=None,
):
    """
    Creates a json file of metadata for each table named in tables

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
        Database connection object. By default, creates one from db_settings.json

    Returns
    -------
    None
        But creates a json file for each table in the database
    """

    if not connection:
        connection = create_database_connection("db_settings.json")
    cursor = connection.cursor()

    for table in tables:
        cursor.execute(f"SELECT * FROM {database}.{table}")
        description = cursor.description
        primary_key_fields = get_primary_key_fields(table=table, cursor=cursor)
        partitions = get_partitions(table=table, cursor=cursor)
        type_lookup = {
            "DATETIME": "datetime",
            "TIMESTAMP": "datetime",
            "STRING": "character",
            "CLOB": "character",
            "BLOB": "character",
            "FIXED_CHAR": "character",
            "LONG_STRING": "character",
            "BINARY": "character",
            "OBJECT": "character",  # ADDED AS TEST - NEED TO CHECK
        }
        columns = []

        if include_op_column:
            columns.append({
                "name": "op",
                "type": "character",
                "description": "Type of change, for rows added by ongoing replication.",
                "nullable": True,
                "enum": ["I", "U", "D"],
            })

        columns.extend(
            [
                {
                    "name": column[0].lower(),
                    "type": f"decimal({column[4]},{column[5]})"
                    if column[1].__name__ == "NUMBER"
                    else type_lookup[column[1].__name__],
                    "description": "",
                    "nullable": bool(column[6]),
                }
                for column in description
            ]
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

        if include_derived_columns is True:
            columns += derived_columns

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
        with open(f"{location}/{table.lower()}.json", "w+") as file:
            json.dump(metadata, file, indent=4)


def get_primary_key_fields(table, cursor):
    statement = (
        "SELECT cols.column_name "
        "FROM all_constraints cons, all_cons_columns cols "
        "WHERE cons.constraint_type = 'P' "
        "AND cons.constraint_name = cols.constraint_name "
        "AND cons.owner = cols.owner "
        "AND cons.status = 'ENABLED' "
        "AND cons.owner = 'EOR' "
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
