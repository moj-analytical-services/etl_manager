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

    def __init__(self, test_to_run=None):
        self.test_to_run = test_to_run

    def cursor(self):
        return TestCursor(self.test_to_run)

    def close(self):
        return

class TestCursor:
    """Pass this to tests instead of a cx_Oracle cursor object.

    The .execute statements are to replace the results of real SQL queries.
    This means it doesn't test the actual SQL - just the Python that follows.
    """

    def __init__(self, test_to_run=None):
        """get_table_meta needs to have description set from the start as it 
        gets passed a cursor that's already done its execute method
        """
        # Rowcount mocks a method from cx_Oracle
        # It specifies how many rows were in the last 'fetch' method called
        self.rowcount = 0

        # Test and counter are for test-running convenience - they aren't in cx_Oracle
        self.test = test_to_run
        self.counter = 0

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
            self.data = [
                (
                    63495,
                    7833,
                    datetime.datetime(2020, 6, 23, 10, 39, 12),
                    "INSTITUTIONAL_REPORT_TRANSFER",
                    "I",
                    12345678,
                    "OBJECT",
                )
            ]

        elif self.test == "document_history":
            self.description = [
                ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
            ]
            self.data = [(7833,)]

        else:
            self.description = []
            self.data = []

    def execute(self, sql, table_name=None, partition_name=None):
        """Each test sets a .test attribute for the cursor. This execute method
        uses that attribute to select the response we'd expect to receive from
        the SQL statement in the function being tested.

        So this is a bit like creating a mock version of the SQL response.
        It does mean the SQL query itself doesn't get tested.
        """
        if self.test == "table_names":
            self.data = [("TEST_TABLE1",), ("TEST_TABLE2",), ("SYS_TABLE",)]

        elif self.test == "primary_key":
            self.data = [("LONG_POSTCODE_ID",)]

        elif self.test == "primary_keys":
            self.data = [("LONG_POSTCODE_ID",), ("TEAM_ID",)]

        elif self.test == "partition":
            self.data = [("P_ADDITIONAL_IDENTIFIER",)]
            self.test = None

        elif self.test == "partitions":
            self.data = [
                ("P_ADDITIONAL_IDENTIFIER",),
                ("P_ADDITIONAL_OFFENCE",),
                ("P_ADDITIONAL_SENTENCE",),
                ("P_ADDRESS",),
                ("P_ADDRESS_ASSESSMENT",),
                ("P_ALIAS",),
                ("P_APPROVED_PREMISES_REFERRAL",),
            ]
            self.test = None

        elif self.test == "subpartition":
            self.data = [
                ("SUBPARTITION_A",),
            ]
            self.test = None

        elif self.test == "subpartitions":
            self.data = [
                ("SUBPARTITION_A",),
                ("SUBPARTITION_B",),
                ("SUBPARTITION_C",),
                ("SUBPARTITION_D",),
            ]
            self.test = None

        elif self.test == "create_table_json":
            self.description = [
                ("TEST_NUMBER", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
                ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
                ("TEST_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
                ("TEST_VARCHAR", cx_Oracle.DB_TYPE_VARCHAR, 30, 30, None, None, 1),
                ("TEST_FLAG", cx_Oracle.DB_TYPE_VARCHAR, 1, 1, None, None, 1),
                ("TEST_ROWID_SKIP", cx_Oracle.DB_TYPE_ROWID, 127, None, 0, -127, 1),
                ("TEST_OBJECT_SKIP", cx_Oracle.DB_TYPE_OBJECT, 127, None, 0, -127, 1),
            ]
            self.data = [
                (
                    63495,
                    7833,
                    datetime.datetime(2020, 6, 23, 10, 39, 12),
                    "INSTITUTIONAL_REPORT_TRANSFER",
                    "I",
                    12345678,
                    "OBJECT",
                )
            ]
            self.test = "empty_table"

        elif self.test == "empty_table":
            self.description = [
                ("TEST_NUMBER", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
                ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
                ("TEST_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
                ("TEST_VARCHAR", cx_Oracle.DB_TYPE_VARCHAR, 30, 30, None, None, 1),
                ("TEST_FLAG", cx_Oracle.DB_TYPE_VARCHAR, 1, 1, None, None, 1),
                ("TEST_ROWID_SKIP", cx_Oracle.DB_TYPE_ROWID, 127, None, 0, -127, 1),
                ("TEST_OBJECT_SKIP", cx_Oracle.DB_TYPE_OBJECT, 127, None, 0, -127, 1),
            ]
            self.data = []
            self.counter += 1
            if self.counter == 2:
                self.test = "second_table"

        elif self.test == "second_table":
            self.description = [
                ("SPG_ERROR_ID", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
                ("ERROR_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
                ("MESSAGE_CRN", cx_Oracle.DB_TYPE_CHAR, 7, 7, None, None, 1),
                ("NOTES", cx_Oracle.DB_TYPE_CLOB, None, None, None, None, 1),
                ("INCIDENT_ID", cx_Oracle.DB_TYPE_VARCHAR, 100, 100, None, None, 1),
            ]

            self.data = [
                (
                    198984,
                    datetime.datetime(2018, 8, 2, 14, 49, 21),
                    "E160306",
                    "CLOB TEXT",
                    1500148234,
                )
            ]
            self.test = "empty_table"

        else:
            self.data = []

    def fetchall(self):
        """Returns everything from the mocked query response.
        """
        self.rowcount = len(self.data)
        return self.data

    def fetchone(self):
        """Returns first row of the mocked query response and removes that row.
        """
        if self.data:
            self.rowcount = 1
            return self.data.pop()
        else:
            self.rowcount = 0
            return None

    def close(self):
        return
