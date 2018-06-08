# -*- coding: utf-8 -*-

"""
Testing DatabaseMeta, TableMeta
"""

import unittest
from etl_manager.meta import DatabaseMeta, TableMeta
from etl_manager.utils import _end_with_slash, _validate_string, _glue_client, _read_json
from etl_manager.etl import GlueJob
import boto3

class UtilsTest(unittest.TestCase) :
    """
    Test packages utilities functions
    """
    def test_meta_funs(self) :
        self.assertEqual(_end_with_slash('no_slash'), 'no_slash/')
        self.assertEqual(_end_with_slash('slash/'), 'slash/')
        self.assertRaises(ValueError, _validate_string, "UPPER")
        self.assertRaises(ValueError, _validate_string, "test:!@")
        self.assertEqual(_validate_string("test:!@", ":!@"), None)

class GlueTest(unittest.TestCase) :
    """
    Test the GlueJob class
    """
    def test_init(self) :
        g = GlueJob('example/glue_jobs/simple_etl_job/', bucket = 'alpha-everyone', job_role = 'alpha_user_isichei', job_arguments={'--test_arg': 'this is a test'})

        self.assertEqual(g.resources, ['example/glue_jobs/simple_etl_job/glue_resources/employees.json', 'example/glue_jobs/shared_job_resources/glue_resources/teams.json'])
        self.assertEqual(g.py_resources, ['example/glue_jobs/shared_job_resources/glue_py_resources/my_dummy_utils.zip'])
        self.assertEqual(g.job_name, 'simple_etl_job')
        self.assertEqual(g.bucket, "alpha-everyone")
        self.assertEqual(g.job_role, 'alpha_user_isichei')
        self.assertEqual(g.github_zip_urls, ['https://github.com/moj-analytical-services/gluejobutils/archive/master.zip'])
        self.assertEqual(g.job_arguments["--test_arg"], 'this is a test')
        self.assertEqual(g.github_py_resources, [])
        self.assertEqual(g.max_retries, 0)
        self.assertEqual(g.max_concurrent_runs, 1)
        self.assertEqual(g.allocated_capacity, 2)

        g2 = GlueJob('example/glue_jobs/simple_etl_job/', bucket = 'alpha-everyone', job_role = 'alpha_user_isichei', include_shared_job_resources=False)
        self.assertEqual(g2.resources, ['example/glue_jobs/simple_etl_job/glue_resources/employees.json'])
        self.assertEqual(g2.py_resources, [])

        self.assertTrue("_GlueJobs_" in g2.job_arguments['--metadata_base_path'])

    def test_db_value_properties(self) :
        g = GlueJob('example/glue_jobs/simple_etl_job/', bucket = 'alpha-everyone', job_role = 'alpha_user_isichei', job_arguments={'--test_arg': 'this is a test'})

        g.job_name = 'changed_job_name'
        self.assertEqual(g.job_name, 'changed_job_name')

        g.bucket = "new-bucket"
        self.assertEqual(g.bucket, "new-bucket")
        with self.assertRaises(ValueError):
            g.bucket = "s3://new-bucket"
            g.bucket = "new_bucket"

        g.job_role = 'alpha_new_user'
        self.assertEqual(g.job_role, 'alpha_new_user')

        g.job_arguments = {"--new_args" : "something"}
        self.assertEqual(g.job_arguments["--new_args"], "something")

        with self.assertRaises(ValueError) :
            g.job_arguments = "not a dict"
            g.job_arguments = {"--JOB_NAME" : "new_job_name"}
            g.job_arguments = {"no_dash" : "test"}


class TableTest(unittest.TestCase):

    def test_table_init(self):
        tm = TableMeta("example/meta_data/db1/teams.json")

        self.assertTrue(tm.database is None)

        gtd = tm.glue_table_definition("full_db_path")
        self.assertTrue(gtd["StorageDescriptor"]["Location"] == 'full_db_path/teams/')



class DatabaseMetaTest(unittest.TestCase):
    """
    Test the Databasse_Meta class
    """

    def test_init(self) :
        db = DatabaseMeta('example/meta_data/db1/')

        self.assertEqual(db.name, 'workforce')
        self.assertEqual(db.description, 'Example database')
        self.assertEqual(db.bucket, 'my-bucket')
        self.assertEqual(db.base_folder, "my_folder/")
        self.assertEqual(db.location, 'database/database1/')
        self.assertEqual(db.db_suffix, '_dev')

        db2 = DatabaseMeta('example/meta_data/db1/', db_suffix='_test')
        self.assertEqual(db2.db_suffix, '_test')


    def test_db_value_properties(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        db.name = 'new_name'
        self.assertEqual(db.name,'new_name')
        db.description = 'new description'
        self.assertEqual(db.description,'new description')
        db.bucket = 'new-bucket'
        self.assertEqual(db.bucket, 'new-bucket')
        db.base_folder = 'this/is/a/base/folder/'
        self.assertEqual(db.base_folder, 'this/is/a/base/folder/')
        db.location = 'new/folder/location'
        self.assertEqual(db.location, 'new/folder/location/')
        db.db_suffix = 'new_suffix'
        self.assertEqual(db.db_suffix, 'new_suffix')

    def test_table_to_dict(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        test_dict = _read_json('example/meta_data/db1/teams.json')
        self.assertDictEqual(test_dict, db.table('teams').to_dict())

    def test_db_table_names(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        t = all(t in ['teams', 'employees'] for t in db.table_names)
        self.assertTrue(t)

    def test_db_glue_name(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        self.assertEqual(db.name, 'workforce_dev')

    def test_db_s3_database_path(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        self.assertEqual(db.s3_database_path, 's3://my-bucket/my_folder_dev/database/database1/')

    def test_db_table(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        self.assertTrue(isinstance(db.table('employees'), TableMeta))
        self.assertRaises(ValueError, db.table, 'not_a_table_name')

    def test_add_remove_table(self) :
        db = DatabaseMeta('example/meta_data/db1/')
        self.assertRaises(ValueError, db.remove_table, 'not_a_table')
        db.remove_table('employees')
        tns = db.table_names
        self.assertEqual(tns[0], 'teams')

        emp_table = TableMeta('example/meta_data/db1/employees.json')
        db.add_table(emp_table)
        t = all(t in ['teams', 'employees'] for t in db.table_names)
        self.assertTrue(t)

        self.assertRaises(ValueError, db.add_table, 'not a table obj')
        self.assertRaises(ValueError, db.add_table, emp_table)

    def test_location(self):
        db = DatabaseMeta('example/meta_data/db1/')
        tbl = db.table('teams')
        gtd = tbl.glue_table_definition()
        location = gtd["StorageDescriptor"]["Location"]
        self.assertTrue(location == 's3://my-bucket/my_folder_dev/database/database1/teams/')


    def test_glue_database_creation(self) :
        session = boto3.Session()
        credentials = session.get_credentials()
        has_access_key = True
        try :
            ac = credentials.access_key
        except :
            has_access_key = False

        if has_access_key :
            db = DatabaseMeta('example/meta_data/db1/', db_suffix = '_unit_test_')
            db.create_glue_database()
            resp = _glue_client.get_tables(DatabaseName = db.name)
            test_created = all([r['Name'] in db.table_names for r in resp['TableList']])
            self.assertTrue(test_created, msg= "Note this requires user to have correct credentials to create a glue database")
            self.assertEqual(db.delete_glue_database(), 'database deleted')
            self.assertEqual(db.delete_glue_database(), 'Cannot delete as database not found in glue catalogue')
        else :
            print("\n***\nCANNOT RUN THIS UNIT TEST AS DO NOT HAVE ACCESS TO AWS.\n***\nskipping ...")
            self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
