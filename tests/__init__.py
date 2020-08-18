import unittest
import boto3
import datetime
import cx_Oracle

from botocore.exceptions import NoCredentialsError


class BotoTester(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        session = boto3.Session()
        credentials = session.get_credentials()
        has_access_key = True
        try:
            __ = credentials.access_key
        except:
            has_access_key = False

        cls.has_access_key = has_access_key

    def skip_test_if_no_creds(self):
        if not self.has_access_key:
            self.skipTest("No boto credentials found")


class TestConnection:
    """Pass this to tests instead of a cx_Oracle connection object
    """
    def cursor(self):
        return TestCursor("table_names")


class TestCursor:
    """Pass this to tests instead of a cx_Oracle cursor object.

    The .execute statements are to replace the results of real SQL queries.
    This means it doesn't test the actual SQL - just the Python that follows.
    """
    def __init__(self, test_to_run):
        self.data = []
        self.description = []
        self.test = test_to_run

        if self.test == "get_table_meta":
            self.description = [
                ("TEST_NUMBER", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
                ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
                ("TEST_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
                ("TEST_VARCHAR", cx_Oracle.DB_TYPE_VARCHAR, 30, 30, None, None, 1),
                ("TEST_FLAG", cx_Oracle.DB_TYPE_VARCHAR, 1, 1, None, None, 1),
                ("TEST_ROWID_SKIP", cx_Oracle.DB_TYPE_ROWID, 127, None, 0, -127, 1),
                ("TEST_OBJECT_SKIP", cx_Oracle.DB_TYPE_OBJECT, 127, None, 0, -127, 1),
            ]
            self.data = [(
                63495,
                7833,
                datetime.datetime(2020, 6, 23, 10, 39, 12),
                "INSTITUTIONAL_REPORT_TRANSFER",
                "I",
            )]

    def execute(self, sql, table_name=None):
        if self.test == "table_names":
            self.data = [("TEST_TABLE1",), ("TEST_TABLE2",), ("SYS_TABLE",)]

        else:
            self.data = []

    def fetchall(self):
        return self.data

    def fetchone(self):
        return self.data.pop()
