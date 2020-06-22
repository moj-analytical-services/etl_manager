# Example job tests access to all files passed to the job runner class
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils.s3 import read_json_from_s3


args = getResolvedOptions(sys.argv, ["JOB_NAME", "metadata_path", "test_arg"])

print("JOB SPECS...")
print("JOB_NAME: ", args["JOB_NAME"])
print("test argument: ", args["test_arg"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

meta_employees = read_json_from_s3(
    os.path.join(args["metadata_path"], "employees.json")
)
meta_teams = read_json_from_s3(os.path.join(args["metadata_path"], "teams.json"))

spark.read.csv("s3://data_bucket/employees/").createOrReplaceTempView("emp")
spark.read.csv("s3://data_bucket/teams/").createOrReplaceTempView("team")

df = spark.sql("SELECT * FROM emp LEFT JOIN team ON emp.employee_id = team.employee_id")

df.write("s3://data_bucket/join/")

job.commit()
