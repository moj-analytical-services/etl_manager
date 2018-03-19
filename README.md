# mojdbtemplate
A python package to create a database on the platform using our moj data warehousing framework.

The main functionality of this package is to sync and run jobs on AWS.

## Rules
- Package does not need to be able to run on glue. Therefore python 3 is fine.
- Work from folder e.g. v1
- Running on dev is default
- Only the code specifies the dev - you never have meta data or folders renamed locally or on github
- Every function needs to be unit tested
- Use data engineering warehouse template as unit tests for functions
- All of the data dependencies of the job should be ran from s3. Even if the job is ran on python locally code should download data from s3, process it and upload to s3

 
