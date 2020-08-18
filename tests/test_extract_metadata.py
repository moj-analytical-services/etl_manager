import unittest
import json
import os

from tests import TestConnection, TestCursor
from etl_manager.extract_metadata import (
    get_table_names,
    create_database_json,
    create_table_json,
    get_table_meta,
)


class TestMetadata(unittest.TestCase):
    def test_get_table_names(self):
        """Check function ignores SYS_ tables
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
        """Tests option flags and tests all data types convert as intended
        Partitions and primary key fields tested separately
        """
        # All flag parameters set to False
        output_no_flags = get_table_meta(
            TestCursor("get_table_meta"),
            table="TEST_TABLE1",
            include_op_column=False,
            include_derived_columns=False,
            include_objects=False,
        )

        columns_no_flags = [
            {
                "name": "test_number",
                "type": "decimal(38,0)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_id",
                "type": "decimal(0,-127)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_date",
                "type": "datetime",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_varchar",
                "type": "character",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_flag",
                "type": "character",
                "description": "",
                "nullable": True,
            },
        ]
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

        # All parameter flags set to True
        output_all_flags = get_table_meta(
            TestCursor("get_table_meta"),
            table="TEST_TABLE1",
            include_op_column=True,
            include_derived_columns=True,
            include_objects=True,
        )

        columns_all_flags = [
            {
                "name": "op",
                "type": "character",
                "description": "Type of change, for rows added by ongoing replication.",
                "nullable": True,
                "enum": ["I", "U", "D"],
            },
            {
                "name": "test_number",
                "type": "decimal(38,0)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_id",
                "type": "decimal(0,-127)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_date",
                "type": "datetime",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_varchar",
                "type": "character",
                "description": "",
                "nullable": True,
            },
            {
                "name": "test_flag",
                "type": "character",
                "description": "",
                "nullable": True,
            },
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
        expected_all_flags = {
            "$schema": (
                "https://moj-analytical-services.github.io/metadata_schema/table/"
                "v1.1.0.json"
            ),
            "name": "test_table1",
            "description": "",
            "data_format": "parquet",
            "columns": columns_all_flags,
            "location": "TEST_TABLE1/",
            "partitions": None,
            "primary_key_fields": None,
        }

        self.assertEqual(output_no_flags, expected_no_flags)
        self.assertEqual(output_all_flags, expected_all_flags)

    def test_get_primary_key_fields(self):
        return

    def test_get_partitions(self):
        return

    def test_get_subpartitions(self):
        return
