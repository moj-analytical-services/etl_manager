import unittest
import json
import os

from tests import TestConnection, TestCursor
from etl_manager.extract_metadata import (
    get_table_names,
    create_database_json,
    create_table_json,
    get_table_meta,
    get_primary_key_fields,
    get_partitions,
    get_subpartitions,
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
        """Tests option flags, document_history tables and data type conversion
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
                "name": "test_object_skip",
                "type": "array<character>",
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

        # DOCUMENT_HISTORY table
        output_doc_history = get_table_meta(
            TestCursor("document_history"),
            table="DOCUMENT_HISTORY",
            include_op_column=False,
            include_derived_columns=False,
            include_objects=False,
        )

        columns_doc_history = [
            {
                "name": "test_id",
                "type": "decimal(0,-127)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "mojap_document_path",
                "type": "character",
                "description": "The path to the document",
                "nullable": True,
            },
        ]
        expected_doc_history = {
            "$schema": (
                "https://moj-analytical-services.github.io/metadata_schema/table/"
                "v1.1.0.json"
            ),
            "name": "document_history",
            "description": "",
            "data_format": "parquet",
            "columns": columns_doc_history,
            "location": "DOCUMENT_HISTORY/",
            "partitions": None,
            "primary_key_fields": None,
        }

        self.assertEqual(output_no_flags, expected_no_flags)
        self.assertEqual(output_all_flags, expected_all_flags)
        self.assertEqual(output_doc_history, expected_doc_history)

    def test_get_primary_key_fields(self):
        """Tests that it formats the output correctly
        Doesn't check that the SQL results are right
        """
        output_key = get_primary_key_fields("TEST_TABLE_KEY", TestCursor("primary_key"))
        output_keys = get_primary_key_fields(
            "TEST_TABLE_KEYS", TestCursor("primary_keys")
        )
        output_no_keys = get_primary_key_fields(
            "TEST_TABLE_NO_KEYS", TestCursor("no_primary_keys")
        )

        expected_key = ("long_postcode_id",)
        expected_keys = ("long_postcode_id", "team_id")
        expected_no_keys = None

        self.assertEqual(output_key, expected_key)
        self.assertEqual(output_keys, expected_keys)
        self.assertEqual(output_no_keys, expected_no_keys)

    def test_get_partitions(self):
        """Tests that partitions are returned correctly. 
        Subpartitions are tested separately. 
        """
        output_partition = get_partitions("PARTITION_TEST", TestCursor("partition"))
        output_partitions = get_partitions("PARTITIONS_TEST", TestCursor("partitions"))
        output_no_partitions = get_partitions("NO_PARTITIONS_TEST", TestCursor())

        expected_partition = [{
            "name": "P_ADDITIONAL_IDENTIFIER", "subpartitions": None
            }]
        expected_partitions = [
            {"name": "P_ADDITIONAL_IDENTIFIER", "subpartitions": None},
            {"name": "P_ADDITIONAL_OFFENCE", "subpartitions": None},
            {"name": "P_ADDITIONAL_SENTENCE", "subpartitions": None},
            {"name": "P_ADDRESS", "subpartitions": None},
            {"name": "P_ADDRESS_ASSESSMENT", "subpartitions": None},
            {"name": "P_ALIAS", "subpartitions": None},
            {"name": "P_APPROVED_PREMISES_REFERRAL", "subpartitions": None},
        ]
        expected_no_partitions = None

        self.assertEqual(output_partition, expected_partition)
        self.assertEqual(output_partitions, expected_partitions)
        self.assertEqual(output_no_partitions, expected_no_partitions)

    def test_get_subpartitions(self):
        return
