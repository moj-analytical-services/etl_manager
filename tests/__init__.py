import unittest
import boto3
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
