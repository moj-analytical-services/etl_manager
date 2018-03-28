# Example job tests access to all files passed to the job runner class
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils import cleaning
from my_dummy_utils.read_json import read_json

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'test_arg'])

print "JOB SPECS..."
print "JOB_NAME: ", args["JOB_NAME"]
print "test argument: ", args["test_arg"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = 'alpha-dag-crest-data-engineering'

meta_employees = read_json(os.path.join(os.getcwd(), "employees.json"))
meta_teams = read_json(os.path.join(os.getcwd(), "teams.json"))

job.commit()