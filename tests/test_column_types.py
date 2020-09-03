import os
import json
import unittest
from unittest import mock

from tests import BotoTester
from parameterized import parameterized

from etl_manager.meta import read_database_folder
from etl_manager.utils import data_type_is_regex

from etl_manager.meta import DatabaseMeta, TableMeta


class ColumnTypesTest(BotoTester):
    @mock.patch("etl_manager.meta._glue_client.create_table")
    def test_can_create_glue_table(self, mock_client_create_table):
        self.skip_test_if_no_creds()
        db = read_database_folder(
            os.path.join(os.path.dirname(__file__), "data/data_types/")
        )
        db.create_glue_database(delete_if_exists=True)
        self.assertTrue(mock_client_create_table.called)

    @mock.patch("etl_manager.meta._glue_client.create_table")
    def test_create_tables_using_etl_manager_api(self, mock_client_create_table):
        self.skip_test_if_no_creds()
        # Create database meta object
        db = DatabaseMeta(
            name="test_data_types",
            bucket="alpha-test-meta-data",
            base_folder="database/test",
        )

        # Create table meta object

        tab = TableMeta(name="test_table", location="test_table/", data_format="json")

        path = os.path.join(
            os.path.dirname(__file__), "data/data_types/test_table.json"
        )
        with open(path) as f:
            table_dict = json.load(f)

        for c in table_dict["columns"]:
            tab.add_column(c["name"], c["type"], description=c["description"])

        self.assertRaises(ValueError, tab.add_column, "bad_col", "array()", "")

        db.add_table(tab)

    @parameterized.expand(
        [
            ("character", True),
            ("int", True),
            ("long", True),
            ("float", True),
            ("double", True),
            ("decimal(38,0)", True),
            ("date", True),
            ("datetime", True),
            ("boolean", True),
            ("struct", False),
            ("array", False),
            ("struct<num:int>", True),
            ("array<int>", True),
            ("array<array<int>>", True),
            ("struct<num:int,newnum:int>", True),
            ("struct<num:int,arr:array<int>>", True),
            ("array<struct<num:int,desc:character>>", True),
            ("struct<num:int,desc:character>", True),
            ("array<decimal(38,0)>", True),
        ]
    )
    def test_col_type_recursive_regex(self, col_type, expected):
        self.assertEqual(data_type_is_regex(col_type), expected)
