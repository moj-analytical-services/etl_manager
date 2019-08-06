import os
import unittest
from unittest import mock

from parameterized import parameterized

from etl_manager.meta import read_database_folder
from etl_manager.utils import data_type_is_regex


class ColumnTypesTest(unittest.TestCase):
    @mock.patch("etl_manager.meta._glue_client.create_table")
    def test_can_create_glue_table(self, mock_client_create_table):
        db = read_database_folder(os.path.join(os.path.dirname(__file__), "data/data_types/"))
        db.create_glue_database(delete_if_exists=True)
        self.assertTrue(mock_client_create_table.called)

    @parameterized.expand([
        ("character", True),
        ("int", True),
        ("long", True),
        ("float", True),
        ("double", True),
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
    ])
    def test_col_type_recursive_regex(self, col_type, expected):
        self.assertEqual(data_type_is_regex(col_type), expected)
