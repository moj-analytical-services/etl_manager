import unittest
import datetime
import json
import os
import cx_Oracle

from etl_manager.extract_metadata import (
    get_table_names,
    create_database_json,
    create_table_json,
    get_table_meta,
)


class TestConnection:
    def cursor(self):
        return TestCursor("table_names")


class TestCursor:
    def __init__(self, test_to_run):
        self.data = []
        self.test = test_to_run
        self.description = []

    def execute(self, sql, table_name=None):
        if self.test == "table_names":
            self.data = [("TEST_TABLE1",), ("TEST_TABLE2",), ("SYS_TABLE",)]

        elif self.test == "get_table_meta":
            self.description = [
                ("TEST_NUMBER", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
                ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
                ("TEST_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
                ("TEST_VARCHAR", cx_Oracle.DB_TYPE_VARCHAR, 30, 30, None, None, 1),
                ("TEST_FLAG", cx_Oracle.DB_TYPE_VARCHAR, 1, 1, None, None, 1),
            ]
            self.data = [(
                63495,
                7833,
                datetime.datetime(2020, 6, 23, 10, 39, 12),
                "INSTITUTIONAL_REPORT_TRANSFER",
                "I",
            )]
            self.test = "no_primary_keys"

        elif self.test == "no_primary_keys":
            # set appropriately
            self.data = []
            self.test = "no_partitions"

        elif self.test == "no_partitions":
            # set appropriately
            self.data = []

    def fetchall(self):
        return self.data

    def fetchone(self):
        return self.data.pop()


class TestMetadata(unittest.TestCase):
    def test_get_table_names(self):
        """Check it drops SYS_ tables
        Doesn't test the SQL query
        """
        result = get_table_names("TEST_DB", TestConnection())
        self.assertEqual(result, ["TEST_TABLE1", "TEST_TABLE2"])

    def test_create_database_json(self):
        """Tests json file is created with correct content
        """
        try:
            create_database_json(
                "This is a database",
                "test_database",
                "bucket-name",
                "hmpps/delius/DELIUS_APP_SCHEMA",
                "./tests/test_metadata",
            )
            with open("tests/test_metadata/database.json", "r") as f:
                output = json.load(f)
        finally:
            os.remove("tests/test_metadata/database.json")

        with open("tests/test_metadata/database_expected.json", "r") as f:
            expected = json.load(f)

        self.assertEqual(output, expected)

    def test_create_table_json(self):
        """
        
        create_table_json(
            tables=["TEST_TABLE1", "TEST_TABLE2"],
            database="TEST_DB",
            location="./tests/test_metadata",
            include_op_column=True,
            include_derived_columns=False,
            connection=TestConnection(),
        )
        # COME BACK TO THIS ONE AFTER DOING TABLE META
        """
        return

    def test_get_table_meta(self):
        # Do I need to patch or mock the partition and primary key function calls?
        """Tests option flags and tests all data types convert as intended
        Partitions and primary key fields tested separately
        """
        output_no_flags = get_table_meta(
            TestCursor("get_table_meta"),
            table="TEST_TABLE1",
            include_op_column=False,
            include_derived_columns=False,
            include_objects=False,
        )

        columns_no_flags = []
        expected_no_flags = {
            "$schema": (
                "https://moj-analytical-services.github.io/metadata_schema/table/"
                "v1.1.0.json"
            ),
            "name": "test_table1",
            "description": "",
            "data_format": "parquet",
            "columns": columns_no_flags,
            "location": "TEST_TABLE1/",
            "partitions": None,
            "primary_key_fields": None,
        }
        self.assertEqual(output_no_flags, expected_no_flags)

    def test_get_primary_key_fields(self):
        return

    def test_get_partitions(self):
        return

    def test_get_subpartitions(self):
        return
