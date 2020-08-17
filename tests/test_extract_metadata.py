import unittest
import json
import os

from etl_manager.extract_metadata import get_table_names, create_database_json


class TestConnection:
    def cursor(self, scrollable=False):
        return TestCursor()


class TestCursor:
    def __init__(self):
        self.queried = []
        self.mock_data = {
            "TEST_DB": {
                "TEST_TABLE1": "data",
                "TEST_TABLE2": "data2",
                "SYS_TABLE": "Nope",
            }
        }

    def execute(self, sql):
        if sql.startswith("SELECT table_name FROM"):
            database = sql.split("= '", 1)[1][:-1]
            self.queried = [(name,) for name in self.mock_data[database].keys()]

        else:
            words = sql.split()
            for w in words:
                if "." in w:
                    parts = w.split(".")
                    database = parts[0]
                    table = parts[1]
                    self.queried = self.mock_data[database][table]

    def fetchall(self):
        return self.queried

    def fetchone(self):
        return self.queried.pop()


class TestMetadata(unittest.TestCase):
    def test_get_table_names(self):
        result = get_table_names("TEST_DB", TestConnection())
        self.assertEqual(result, ["TEST_TABLE1", "TEST_TABLE2"])

    def test_create_database_json(self):
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
        return

    def test_get_table_meta(self):
        return

    def test_get_primary_key_fields(self):
        return

    def test_get_partitions(self):
        return

    def test_get_subpartitions(self):
        return
