import os
import unittest

from tests import BotoTester
from parameterized import parameterized

from etl_manager.meta import read_database_folder, get_existing_database_from_glue_catalogue, TableMeta
from etl_manager.utils import data_type_is_regex
import time
import boto3

def run_athena_sql(sql):
    athena_client = boto3.client("athena", "eu-west-1")

    response = athena_client.start_query_execution(
            QueryString=sql, ResultConfiguration={"OutputLocation": "s3://alpha-test-meta-data/athena_out/"}
        )

    sleep_time = 2
    counter = 0
    timeout = None
    while True:
        athena_status = athena_client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        if athena_status["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
            break
        elif athena_status["QueryExecution"]["Status"]["State"] in [
            "QUEUED",
            "RUNNING",
        ]:
            
            time.sleep(sleep_time)
        elif athena_status["QueryExecution"]["Status"]["State"] == "FAILED":
            scr = athena_status["QueryExecution"]["Status"]["StateChangeReason"]
            raise ValueError("athena failed - response error:\n {}".format(scr))
        else:
            raise ValueError(
                """
            athena failed - unknown reason (printing full response):
            {}
            """.format(
                    athena_status
                )
            )

        counter += 1
        if timeout:
            if counter * sleep_time > timeout:
                raise ValueError("athena timed out")

    result_response = athena_client.get_query_results(
        QueryExecutionId=athena_status["QueryExecution"]["QueryExecutionId"],
        MaxResults=1,
    )


class CreateUpdateTest(BotoTester):
    
    def test_create_database(self):
        
        self.skip_test_if_no_creds()
        db = read_database_folder(os.path.join(os.path.dirname(__file__), "data/data_types/"))
        db.create_glue_database(delete_if_exists=True)
        
        sql = """
        select * from test_data_types.test_table
        """

        # run_athena_sql(sql)

        db = get_existing_database_from_glue_catalogue("test_data_types")

        tab = TableMeta(name = 'test_table_2', location = 'database/test/test_table/', data_format="json")
        tab.add_column("robin_entity_id", "int", description = "an ID for each entity")
        db.add_table(tab)

        db.update_glue_database()

        sql = """
        select * from test_data_types.test_table_2
        """
        run_athena_sql(sql)





