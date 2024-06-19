# -*- coding: utf-8 -*-

"""
Testing DatabaseMeta, TableMeta
"""

import tempfile
import os
import urllib
import json
import unittest

from etl_manager.meta import (
    DatabaseMeta,
    TableMeta,
    read_database_folder,
    read_table_json,
    _agnostic_to_glue_spark_dict,
    MetaColumnTypeMismatch,
    _get_spec,
    _table_json_schema,
    _web_link_to_table_json_schema,
)
from etl_manager.utils import (
    _end_with_slash,
    _validate_string,
    _glue_client,
    read_json,
    _remove_final_slash,
    trim_complex_type,
)
from etl_manager.etl import GlueJob

from tests import BotoTester


class UtilsTest(BotoTester):
    """
    Test packages utilities functions
    """

    def test_meta_funs(self):
        self.assertEqual(_end_with_slash("no_slash"), "no_slash/")
        self.assertEqual(_end_with_slash("slash/"), "slash/")
        self.assertRaises(ValueError, _validate_string, "UPPER")
        self.assertRaises(ValueError, _validate_string, "test:!@")
        self.assertEqual(_validate_string("test:!@", ":!@"), None)
        self.assertEqual(_remove_final_slash("hello/"), "hello")
        self.assertEqual(_remove_final_slash("hello"), "hello")

    def test_trim_complex_type(self):
        self.assertEqual(trim_complex_type("decimal(38,0)"), "decimal")
        self.assertEqual(trim_complex_type("array<decimal(38,0)>"), "array")
        self.assertEqual(
            trim_complex_type("struct<arr_key:array<decimal(38,0)>>"), "struct"
        )


class GlueTest(BotoTester):
    """
    Test the GlueJob class
    """

    def test_init(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            job_arguments={"--test_arg": "this is a test"},
        )

        self.assertEqual(
            g.resources,
            [
                "example/glue_jobs/simple_etl_job/glue_resources/employees.json",
                "example/glue_jobs/shared_job_resources/glue_resources/teams.json",
            ],
        )
        self.assertEqual(
            g.py_resources,
            [
                "example/glue_jobs/shared_job_resources/glue_py_resources/"
                "my_dummy_utils.zip"
            ],
        )

        self.assertEqual(
            set(g.jars),
            set(
                [
                    "example/glue_jobs/simple_etl_job/glue_jars/j1.jar",
                    "example/glue_jobs/simple_etl_job/glue_jars/j2.jar",
                ]
            ),
        )
        self.assertEqual(g.job_name, "simple_etl_job")
        self.assertEqual(g.bucket, "alpha-everyone")
        self.assertEqual(g.job_role, "alpha_user_isichei")
        self.assertEqual(
            g.github_zip_urls,
            [
                "https://github.com/moj-analytical-services/gluejobutils/archive/"
                "master.zip"
            ],
        )
        self.assertEqual(g.job_arguments["--test_arg"], "this is a test")
        self.assertEqual(g.github_py_resources, [])
        self.assertEqual(g.max_retries, 0)
        self.assertEqual(g.max_concurrent_runs, 1)
        self.assertEqual(g.worker_type, "G.1X")
        self.assertEqual(g.number_of_workers, 2)
        self.assertEqual(g.glue_version, "2.0")
        self.assertEqual(g.python_version, "3")
        self.assertEqual(g.pip_requirements, None)

        jobdef = g._job_definition()
        self.assertTrue("j2.jar" in jobdef["DefaultArguments"]["--extra-jars"])

        g2 = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            include_shared_job_resources=False,
        )
        self.assertEqual(
            g2.resources,
            ["example/glue_jobs/simple_etl_job/glue_resources/employees.json"],
        )
        self.assertEqual(g2.py_resources, [])

        self.assertTrue("_GlueJobs_" in g2.job_arguments["--metadata_base_path"])

    def test_glue_param_error(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            job_arguments={"--test_arg": "this is a test"},
        )
        with self.assertRaises(ValueError):
            g.job_arguments = "--bad_job_argument1"
        with self.assertRaises(ValueError):
            g.job_arguments = {"bad_job_argument2": "test"}
        with self.assertRaises(ValueError):
            g.job_arguments = {"--JOB_NAME": "new_job_name"}

    def test_db_value_properties(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            job_arguments={"--test_arg": "this is a test"},
        )

        g.job_name = "changed_job_name"
        self.assertEqual(g.job_name, "changed_job_name")

        g.bucket = "new-bucket"
        self.assertEqual(g.bucket, "new-bucket")
        with self.assertRaises(ValueError):
            g.bucket = "s3://new-bucket"

        g.job_role = "alpha_new_user"
        self.assertEqual(g.job_role, "alpha_new_user")

        g.job_arguments = {"--new_args": "something"}
        self.assertEqual(g.job_arguments["--new_args"], "something")

    def test_timeout(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            job_arguments={"--test_arg": "this is a test"},
        )

        self.assertEqual(g._job_definition()["Timeout"], 1363)

        g.number_of_workers = 5
        g.worker_type = "G.2X"

        self.assertEqual(g._job_definition()["Timeout"], 272)

        g.number_of_workers = 40
        g.worker_type = "G.1X"

        self.assertEqual(g._job_definition()["Timeout"], 68)

        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            job_arguments={"--test_arg": "this is a test"},
            timeout_override_minutes=2880,
        )

        g.number_of_workers = 40

        self.assertEqual(g._job_definition()["Timeout"], 2880)

    def test_tags(self):
        # Test default tags
        g1 = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
        )
        self.assertEqual(g1.tags, {})

        # Test overriding default tags
        g1.tags = {"a": "b"}
        self.assertEqual(g1.tags, {"a": "b"})

        # Test tags in job definition
        job_definition = g1._job_definition()
        self.assertEqual(job_definition["Tags"], {"a": "b"})

        # Test setting custom tags
        g2 = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
            tags={"c": "d"},
        )
        self.assertEqual(g2.tags, {"c": "d"})

        # Test disallowed type
        with self.assertRaises(TypeError):
            GlueJob(
                "example/glue_jobs/simple_etl_job/",
                bucket="alpha-everyone",
                job_role="alpha_user_isichei",
                tags=["test"],
            )

    def test_glue_version(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
        )
        for glue_version in ["3.0", "2.0", "1.0", "0.9"]:
            g.glue_version = glue_version
            self.assertEqual(g.glue_version, glue_version)

        with self.assertRaises(TypeError):
            g.glue_version = 1

        with self.assertRaises(ValueError):
            g.glue_version = "1.1"

    def test_python_version(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
        )
        for python_version in ["2", "3"]:
            g.python_version = python_version
            self.assertEqual(g.python_version, python_version)

        with self.assertRaises(TypeError):
            g.python_version = 1

        with self.assertRaises(ValueError):
            g.python_version = "1.1"

    def test_pip_requirements(self):
        g = GlueJob(
            "example/glue_jobs/simple_etl_job/",
            bucket="alpha-everyone",
            job_role="alpha_user_isichei",
        )

        for glue_version in ["1.0", "0.9"]:
            g.glue_version = glue_version
            self.assertEqual(g.pip_requirements, None)

        for glue_version in ["2.0", "3.0"]:
            g.glue_version = glue_version
            for fp in ["gibberish", "requirements"]:
                with self.assertRaises(ValueError) as context:
                    g.pip_requirements = fp


class TableTest(BotoTester):
    def test_table_init(self):
        tm = read_table_json("example/meta_data/db1/teams.json")

        self.assertTrue(tm.database is None)

        gtd = tm.glue_table_definition("full_db_path")
        self.assertTrue(gtd["StorageDescriptor"]["Location"] == "full_db_path/teams/")


class DatabaseMetaTest(BotoTester):
    """
    Test the Databasse_Meta class
    """

    def test_init(self):
        db = DatabaseMeta(
            name="workforce",
            bucket="my-bucket",
            base_folder="database/database1",
            description="Example database",
        )
        self.assertEqual(db.name, "workforce")
        self.assertEqual(db.description, "Example database")
        self.assertEqual(db.bucket, "my-bucket")
        self.assertEqual(db.base_folder, "database/database1")

    def test_read_json(self):
        db = read_database_folder("example/meta_data/db1/")
        self.assertEqual(db.name, "workforce")
        self.assertEqual(db.description, "Example database")
        self.assertEqual(db.bucket, "my-bucket")
        self.assertEqual(db.base_folder, "database/database1")

    def test_db_to_dict(self):
        db = DatabaseMeta(
            name="workforce",
            bucket="my-bucket",
            base_folder="database/database1",
            description="Example database",
        )
        db_dict = read_json("example/meta_data/db1/database.json")
        self.assertDictEqual(db_dict, db.to_dict())

    def test_db_write_to_json(self):
        db = DatabaseMeta(
            name="workforce",
            bucket="my-bucket",
            base_folder="database/database1",
            description="Example database",
        )
        t = TableMeta(name="table1", location="somewhere")
        db.add_table(t)

        with tempfile.TemporaryDirectory() as tmpdirname:
            db.write_to_json(tmpdirname)
            dbr = read_json(os.path.join(tmpdirname, "database.json"))
            tr = read_json(os.path.join(tmpdirname, "table1.json"))

        self.assertDictEqual(dbr, db.to_dict())
        self.assertDictEqual(tr, t.to_dict())

        with tempfile.TemporaryDirectory() as tmpdirname:
            db.write_to_json(tmpdirname, write_tables=False)

            dbr = read_json(os.path.join(tmpdirname, "database.json"))
            self.assertDictEqual(dbr, db.to_dict())

            # Check that only db has been written
            with self.assertRaises(FileNotFoundError):
                tr = read_json(os.path.join(tmpdirname, "table1.json"))

    def test_db_value_properties(self):
        db = read_database_folder("example/meta_data/db1/")
        db.name = "new_name"
        self.assertEqual(db.name, "new_name")
        db.description = "new description"
        self.assertEqual(db.description, "new description")
        db.bucket = "new-bucket"
        self.assertEqual(db.bucket, "new-bucket")
        db.base_folder = "new/folder/location"
        self.assertEqual(db.base_folder, "new/folder/location")

    def test_table_to_dict(self):
        db = read_database_folder("example/meta_data/db1/")
        expected_dict = read_json("example/meta_data/db1/teams.json")
        test_dict = db.table("teams").to_dict()

        # Null out schema as may need changing when on branch but still need to unit
        # test
        expected_dict["$schema"] = ""
        test_dict["$schema"] = ""

        self.assertDictEqual(test_dict, expected_dict)

        # Test file with glue specific
        expected_dict2 = read_json("example/meta_data/db1/pay.json")
        test_dict2 = db.table("pay").to_dict()

        # Null out schema as may need changing when on branch but still need to unit
        # test
        expected_dict2["$schema"] = ""
        test_dict2["$schema"] = ""

        self.assertDictEqual(test_dict2, expected_dict2)

    def test_db_table_names(self):
        db = read_database_folder("example/meta_data/db1/")
        t = all(t in ["teams", "employees", "pay"] for t in db.table_names)
        self.assertTrue(t)

    def test_db_name_validation(self):
        db = read_database_folder("example/meta_data/db1/")
        with self.assertRaises(ValueError):
            db.name = "bad-name"

    def test_db_glue_name(self):
        db = read_database_folder("example/meta_data/db1/")
        self.assertEqual(db.name, "workforce")

        db_dev = read_database_folder("example/meta_data/db1/")
        self.assertEqual(db_dev.name, "workforce")

    def test_db_s3_database_path(self):
        db = read_database_folder("example/meta_data/db1/")
        self.assertEqual(db.s3_database_path, "s3://my-bucket/database/database1")

    def test_db_table(self):
        db = read_database_folder("example/meta_data/db1/")
        self.assertTrue(isinstance(db.table("employees"), TableMeta))
        self.assertRaises(ValueError, db.table, "not_a_table_object")

    def test_glue_specific_table(self):
        t = read_table_json("example/meta_data/db1/pay.json")
        self.assertTrue(
            t.glue_table_definition("db_path")["Parameters"]["skip.header.line.count"]
            == "1"
        )

    def test_glue_table_definition_doesnt_overwrite_base_spec(self):
        expected_dict = _get_spec("base")

        self.assertDictEqual(expected_dict, _get_spec("base"))

    def test_add_remove_table(self):
        db = read_database_folder("example/meta_data/db1/")
        self.assertRaises(ValueError, db.remove_table, "not_a_table")
        db.remove_table("employees")
        tns = db.table_names
        self.assertEqual(set(tns), set(["teams", "pay"]))

        emp_table = read_table_json("example/meta_data/db1/employees.json")
        db.add_table(emp_table)
        t = all(t in ["teams", "employees", "pay"] for t in db.table_names)
        self.assertTrue(t)

        self.assertRaises(ValueError, db.add_table, "not a table obj")
        self.assertRaises(ValueError, db.add_table, emp_table)

    def test_location(self):
        db = read_database_folder("example/meta_data/db1/")
        tbl = db.table("teams")
        gtd = tbl.glue_table_definition()
        location = gtd["StorageDescriptor"]["Location"]
        self.assertTrue(location == "s3://my-bucket/database/database1/teams/")

    def test_glue_database_creation(self):

        self.skip_test_if_no_creds()
        db = read_database_folder("example/meta_data/db1/")
        db_suffix = "_unit_test_"
        db.name = db.name + db_suffix
        db.create_glue_database()
        resp = _glue_client.get_tables(DatabaseName=db.name)
        test_created = all([r["Name"] in db.table_names for r in resp["TableList"]])
        self.assertTrue(
            test_created,
            msg=(
                "Note this requires user to have correct credentials to create a glue "
                "database"
            ),
        )
        self.assertEqual(db.delete_glue_database(), "database deleted")
        self.assertEqual(
            db.delete_glue_database(), "database not found in glue catalogue"
        )

    def test_db_test_column_types_align(self):
        db = read_database_folder("example/meta_data/db1/")
        # Should pass
        db.test_column_types_align()

        db.table("pay").update_column(column_name="employee_id", type="character")

        # Should pass
        db.test_column_types_align(exclude_tables=["pay"])

        # Should fail
        with self.assertRaises(MetaColumnTypeMismatch):
            db.test_column_types_align()


class TableMetaTest(BotoTester):
    """
    Test Table Meta class
    """

    def test_data_type_conversion_against_gluejobutils(self):
        with urllib.request.urlopen(
            "https://raw.githubusercontent.com/moj-analytical-services/gluejobutils/"
            "master/gluejobutils/data/data_type_conversion.json"
        ) as url:
            gluejobutils_data = json.loads(url.read().decode())

        self.assertDictEqual(_agnostic_to_glue_spark_dict, gluejobutils_data)

    def test_null_init(self):
        tm = TableMeta("test_name", location="folder/")

        self.assertEqual(tm.name, "test_name")
        self.assertEqual(tm.description, "")
        self.assertEqual(tm.data_format, "csv")
        self.assertEqual(tm.location, "folder/")
        self.assertEqual(tm.columns, [])
        self.assertEqual(tm.partitions, [])
        self.assertEqual(tm.primary_key, [])
        self.assertEqual(tm.glue_specific, {})

        kwargs = {
            "name": "employees",
            "description": "table containing employee information",
            "data_format": "parquet",
            "location": "employees/",
            "columns": [
                {
                    "name": "employee_id",
                    "type": "int",
                    "description": "an ID for each employee",
                },
                {
                    "name": "employee_name",
                    "type": "character",
                    "description": "name of the employee",
                },
                {
                    "name": "employee_dob",
                    "type": "date",
                    "description": "date of birth for the employee",
                },
            ],
            "primary_key": ["employee_id"],
        }

        tm2 = TableMeta(**kwargs)
        self.assertEqual(tm2.name, kwargs["name"])
        self.assertEqual(tm2.description, kwargs["description"])
        self.assertEqual(tm2.data_format, kwargs["data_format"])
        self.assertEqual(tm2.location, kwargs["location"])
        self.assertEqual(tm2.columns, kwargs["columns"])
        self.assertEqual(tm2.partitions, [])
        self.assertEqual(tm2.primary_key, kwargs["primary_key"])
        self.assertEqual(tm2.glue_specific, {})
        with self.assertRaises(ValueError):
            tm2.database = "not a database obj"

    def test_db_name_validation(self):
        tm = TableMeta("test_name", location="folder/")
        with self.assertRaises(ValueError):
            tm.name = "bad-name"

    def test_column_operations(self):
        kwargs = {
            "name": "employees",
            "description": "table containing employee information",
            "data_format": "parquet",
            "location": "employees/",
            "columns": [
                {
                    "name": "employee_id",
                    "type": "int",
                    "description": "an ID for each employee",
                },
                {
                    "name": "employee_name",
                    "type": "character",
                    "description": "name of the employee",
                },
            ],
        }

        columns_test = [
            {
                "name": "employee_id2",
                "type": "character",
                "description": "a new description",
            },
            {
                "name": "employee_name",
                "type": "character",
                "description": "name of the employee",
            },
        ]

        new_col = {
            "name": "employee_dob",
            "type": "date",
            "description": "date of birth for the employee",
            "nullable": True,
            "pattern": "\d{4}-\d{2}-\d{2}",
            "enum": [
                "a",
                "b",
                "c",
            ],  # yes enums and patterns can conflict - no validation for this
            "sensitivity": "personal_data",
            "redacted": False,
        }

        tm = TableMeta(**kwargs)

        # Test add
        tm.add_column(**new_col)
        new_cols = kwargs["columns"] + [new_col]
        self.assertEqual(tm.columns, new_cols)

        # Test add failure
        with self.assertRaises(ValueError):
            tm.add_column(name="test:!@", type="something", description="")

        # Test remove
        tm.remove_column("employee_dob")
        self.assertEqual(tm.columns, kwargs["columns"])

        # Test remove failure
        with self.assertRaises(ValueError):
            tm.remove_column("j_cole")

        # Test update column failure
        tm.update_column(
            "employee_id",
            name="employee_id2",
            type="character",
            description="a new description",
        )
        self.assertEqual(tm.columns, columns_test)

        with self.assertRaises(ValueError):
            tm.update_column("employee_id2")

        with self.assertRaises(ValueError):
            tm.update_column("j_cole", type="int")

        # Test can update col column with pattern, nullable, enum, sensitivity and
        # redacted properties
        kwargs2 = {
            "name": "employees",
            "description": "table containing employee information",
            "data_format": "parquet",
            "location": "employees/",
            "columns": [
                {
                    "name": "employee_id",
                    "type": "int",
                    "description": "an ID for each employee",
                },
                {
                    "name": "employee_name",
                    "type": "character",
                    "description": "name of the employee",
                },
                {
                    "name": "employee_ethnicity",
                    "type": "character",
                    "description": "ethnicity of the employee",
                },
            ],
        }

        columns_test2 = [
            {
                "name": "employee_id",
                "type": "int",
                "description": "an ID for each employee",
                "pattern": "\d+",
            },
            {
                "name": "employee_name",
                "type": "character",
                "description": "name of the employee",
                "enum": ["john", "sally"],
                "nullable": False,
            },
            {
                "name": "employee_ethnicity",
                "type": "character",
                "description": "ethnicity of the employee",
                "sensitivity": "special_category_data",
                "redacted": False,
            },
        ]

        tm = TableMeta(**kwargs2)
        tm.update_column("employee_id", pattern="\d+")
        tm.update_column("employee_name", enum=["john", "sally"], nullable=False)
        tm.update_column(
            "employee_ethnicity", sensitivity="special_category_data", redacted=False
        )

        self.assertEqual(tm.columns, columns_test2)

        # Test basic validation for pattern, nullable, enum, sensitivity and redacted
        # properties
        with self.assertRaises(TypeError):
            tm.update_column("employee_id", pattern=5)
        with self.assertRaises(TypeError):
            tm.update_column("employee_id", enum=5)
        with self.assertRaises(TypeError):
            tm.update_column("employee_id", nullable=5)
        with self.assertRaises(TypeError):
            tm.update_column("employee_id", sensitivity=5)
        with self.assertRaises(TypeError):
            tm.update_column("employee_id", redacted=5)

        with self.assertRaises(ValueError):
            tm.update_column("employee_id", sensitivity="non-sensitive")

    def test_partition_cols_are_last(self):
        tb = TableMeta(name="test", location="test")

        tb.add_column("p", "int", "")
        tb.add_column("a", "int", "")

        tb.partitions = ["p"]
        self.assertListEqual(tb.column_names, ["a", "p"])

        tb.add_column("b", "int", "")
        self.assertListEqual(tb.column_names, ["a", "b", "p"])

    def test_primary_key(self):
        tm = TableMeta(name="test", location="test")
        tm.add_column("a", "int", "")
        tm.add_column("b", "int", "")

        tm.primary_key = ["a"]
        self.assertListEqual(tm.primary_key, ["a"])

        tm.primary_key = ["b"]
        self.assertListEqual(tm.primary_key, ["b"])

        tm.primary_key = ["a", "b"]
        self.assertListEqual(tm.primary_key, ["a", "b"])

        tm.primary_key = None
        self.assertListEqual(tm.primary_key, [])

        with self.assertRaises(ValueError):
            tm.primary_key = ["c"]

        with self.assertRaises(TypeError):
            tm.primary_key = "a"

    def test_local_schema_matches_web_schema(self):
        with urllib.request.urlopen(_web_link_to_table_json_schema) as url:
            web_schema = json.loads(url.read().decode())
        self.assertDictEqual(_table_json_schema, web_schema)

    def test_table_sensitivity(self):
        tm = TableMeta(name="test", location="test")

        with self.assertRaises(AttributeError):
            tm.sensitivity = ["personal_data"]

        self.assertEqual(tm.sensitivity, [])

        tm.add_column(
            name="employee_name",
            type="character",
            description="The name of the employee",
            sensitivity="personal_data",
        )
        self.assertEqual(tm.sensitivity, ["personal_data"])

        tm.add_column(
            name="employee_ethnicity",
            type="character",
            description="The ethnicity of the employee",
            sensitivity="special_category_data",
        )
        self.assertEqual(tm.sensitivity, ["personal_data", "special_category_data"])

        tm.update_column(
            column_name="employee_ethnicity",
            sensitivity="personal_data",
        )
        self.assertEqual(tm.sensitivity, ["personal_data"])

        for column_name in ["employee_name", "employee_ethnicity"]:
            tm.remove_column(column_name)
        self.assertEqual(tm.sensitivity, [])


if __name__ == "__main__":
    unittest.main()
