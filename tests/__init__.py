import unittest
import boto3


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
    """Pass this to tests instead of a cx_Oracle connection object.

    responses is a list of dictionaries, each mocking a cursor's
    .execute response. It gets passed to the TestCursor on creation.
    """
    def __init__(self, responses=None):
        self.responses = responses or []

    def cursor(self):
        return TestCursor(self.responses)

    def close(self):
        return


class TestCursor:
    """Pass this to tests instead of a cx_Oracle cursor object.

    Use the responses parameter to mock SQL queries. Responses is a list of dicts
    where each dict is the response to one .execute query.
    
    Each dict should have a desc key and a data key. Desc becomes the TestCursor's
    .description attribute, which contains metadata about the columns returned.
    Data becomes the TestCursor's .data attribute, and represents the rows returned 

    Running .execute() mocks the sending of a query by moving the first .responses
    item into the TestCursor's .data and .description attributes. This mimics how
    a real SQL query would update the cursor's description and hold data ready to
    be returned by fetch methods. The actual SQL is ignored. 

    The upshot is that this class is useful for testing functions containing cx_Oracle
    objects, but it can't test the SQL query or database connection.
    """
    def __init__(self, responses=None, description=None):
        """Initialise with responses to queue up responses to .execute queries.
        Initialise with description to mimic a cursor where a .execute query
        has already been made (for example in get_table_meta)
        """
        # Rowcount mocks the .rowcount method from cx_Oracle
        # It specifies how many rows were in the last 'fetch' method called
        self.rowcount = 0
        self.responses = responses or []
        self.description = description or []
        self.data = []

    def execute(self, sql, table_name=None, partition_name=None):
        """Mimics and SQL query by popping the next response_desc and response_data
        into the cursor's description and data attributes
        """
        if self.responses:
            response = self.responses.pop(0)
            self.description = response.get("desc", [])
            self.data = response.get("data", [])
        else:
            self.description = []
            self.data = []

    def fetchall(self):
        """Returns everything from the most recently mocked query response.
        """
        self.rowcount = len(self.data)
        return self.data

    def fetchone(self):
        """Returns first row of the mocked query response and removes that row.
        """
        if self.data:
            self.rowcount = 1
            return self.data.pop(0)
        else:
            self.rowcount = 0
            return None

    def close(self):
        return
