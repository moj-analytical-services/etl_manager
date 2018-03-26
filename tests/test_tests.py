# -*- coding: utf-8 -*-

"""
Testing Database_Meta, Table_Meta
"""

import unittest
from mojdbtemplate.meta import Database_Meta, Table_Meta, _end_with_slash, _validate_string, _glue_client
import boto3

class DatabaseMetaTest(unittest.TestCase):
    """
    Test the Databasse_Meta class
    """

    def test_init(self) :
        db = Database_Meta('example_meta_data/')

        self.assertEqual(db.name, 'workforce')
        self.assertEqual(db.description, 'Example database')
        self.assertEqual(db.bucket, 'my_bucket')
        self.assertEqual(db.base_folder, "my_folder/")
        self.assertEqual(db.location, 'database/database1/')
        self.assertEqual(db.db_suffix, '_dev')

        db2 = Database_Meta('example_meta_data/', db_suffix='_test')
        self.assertEqual(db2.db_suffix, '_test')


    def test_db_value_properties(self) :
        db = Database_Meta('example_meta_data/')
        db.name = 'new_name'
        self.assertEqual(db.name,'new_name')
        db.description = 'new description'
        self.assertEqual(db.description,'new description')
        db.bucket = 'new_bucket'
        self.assertEqual(db.bucket, 'new_bucket')
        db.base_folder = 'this/is/a/base/folder/'
        self.assertEqual(db.base_folder, 'this/is/a/base/folder/')
        db.location = 'new/folder/location'
        self.assertEqual(db.location, 'new/folder/location/')
        db.db_suffix = 'new_suffix'
        self.assertEqual(db.db_suffix, 'new_suffix')

    def test_db_table_names(self) :
        db = Database_Meta('example_meta_data/')
        t = all(t in ['teams', 'employees'] for t in db.table_names)
        self.assertTrue(t)

    def test_db_glue_name(self) :
        db = Database_Meta('example_meta_data/')
        self.assertEqual(db.glue_name, 'workforce_dev')
    
    def test_db_s3_database_path(self) :
        db = Database_Meta('example_meta_data/')
        self.assertEqual(db.s3_database_path, 's3://my_bucket/my_folder_dev/database/database1/')

    def test_db_table(self) :
        db = Database_Meta('example_meta_data/')
        self.assertTrue(isinstance(db.table('employees'), Table_Meta))
        self.assertRaises(ValueError, db.table, 'not_a_table_name')

    def test_add_remove_table(self) :
        db = Database_Meta('example_meta_data/')
        self.assertRaises(ValueError, db.remove_table, 'not_a_table')
        db.remove_table('employees')
        tns = db.table_names
        self.assertEqual(tns[0], 'teams')

        emp_table = Table_Meta('example_meta_data/employees.json')
        db.add_table(emp_table)
        t = all(t in ['teams', 'employees'] for t in db.table_names)
        self.assertTrue(t)

        self.assertRaises(ValueError, db.add_table, 'not a table obj')
        self.assertRaises(ValueError, db.add_table, emp_table)
        
    def test_meta_funs(self) :
        
        self.assertEqual(_end_with_slash('no_slash'), 'no_slash/')
        self.assertEqual(_end_with_slash('slash/'), 'slash/')
        self.assertRaises(ValueError, _validate_string, "UPPER")
        self.assertRaises(ValueError, _validate_string, "test:!@")
        self.assertEqual(_validate_string("test:!@", ":!@"), None)

    def test_glue_database_creation(self) :
        session = boto3.Session()
        credentials = session.get_credentials()
        has_access_key = True
        try :
            ac = credentials.access_key
        except :
            has_access_key = False
        
        if has_access_key :
            db = Database_Meta('example_meta_data/', db_suffix = '_unit_test_')
            db.create_glue_database()
            resp = _glue_client.get_tables(DatabaseName = db.glue_name)
            test_created = all([r['Name'] in db.table_names for r in resp['TableList']])
            self.assertTrue(test_created, msg= "Note this requires user to have correct credentials to create a glue database")
            self.assertEqual(db.delete_glue_database(), 'database deleted')
            self.assertEqual(db.delete_glue_database(), 'Cannot delete as database not found in glue catalogue')
        else :
            print("\n***\nCANNOT RUN THIS UNIT TEST AS DO NOT HAVE ACCESS TO AWS.\n***\nskipping ...")
            self.assertTrue(True)
            
if __name__ == '__main__':
    unittest.main()