# etl_manager

[![Actions Status](https://github.com/moj-analytical-services/etl_manager/workflows/ETL%20Manager/badge.svg)](https://github.com/moj-analytical-services/etl_manager/actions)

A python package that manages our data engineering framework and implements them on AWS Glue.

The main functionality of this package is to interact with AWS Glue to create meta data catalogues and run Glue jobs.

To install:

```bash
pip install etl_manager
```

## Meta Data

Let's say I have a single table (a csv file) and I want to query it using Amazon athena. My csv file is in the following S3 path: `s3://my-bucket/my-table-folder/file.csv`.

file.csv is a table that looks like this:

| col1 | col2 |
|------|------|
| a    | 1    |
| b    | 12   |
| c    | 42   |

As you can see col1 is a string and col2 is a integer.

> **Notes:**
> - for Athena to work your table should not contain a header. So before file.csv is uploaded to S3 you should make sure it has no header.
> - Tables must be in a folder. I.e. the location of your table (`table.location`) should be the parent folder of where you data exists. See example below.

To create a schema for your data to be queried by Athena you can use the following code:

```python
from etl_manager.meta import DatabaseMeta, TableMeta

# Create database meta object
db = DatabaseMeta(name = 'my_database', bucket='my-bucket')

# Create table meta object
tab = TableMeta(name = 'my_table', location = 'my-table-folder')

# Add column defintions to the table
tab.add_column(name = 'col1', 'character', description = 'column contains a letter')
tab.add_column(name = 'col2', 'int', description = 'column contains a number')

# Add table to the database
db.add_table(tab)

# Create the table on AWS glue
db.create_glue_database()
```

Now the table can be queried via SQL e.g. `SELECT * FROM my_database.my_table`

### Meta data structure

Currently at very simple level. Assume you have the following folder structure from example code:

```
meta_data/
--- database.json
--- teams.json
--- employees.json
```

database.json is a special json file that holds the meta data for the database. In our example it looks like this:

```json
{
    "description": "Example database",
    "name": "workforce",
    "bucket": "my-bucket",
    "base_folder": "database/database1"
}
```

When you create your database it will have this `name` and `description`. The `bucket` key specifies where the database exists (and therefore where the tables exist) in S3. The `base_folder` is is the initial path to where the tables exist. If your tables are in folders directly in the bucket (e.g. `s3://my-bucket/table1/`) then you can leave base_folder as an empty string (`""`).

The employees table has an ID for each employee their name and dob. The table meta looks like this:

```json
{
    "$schema" : "https://moj-analytical-services.github.io/metadata_schema/table/v1.0.0.json",
    "name": "employees",
    "description": "table containing employee information",
    "data_format": "parquet",
    "location": "employees/",
    "columns": [
        {
            "name": "employee_id",
            "type": "int",
            "description": "an ID for each employee"
        },
        {
            "name": "employee_name",
            "type": "character",
            "description": "name of the employee"
        },
        {
            "name": "employee_dob",
            "type": "date",
            "description": "date of birth for the employee"
        }
    ]
}
```

**Currently supported data types for your columns currently are:**

`character | int | long | float | double | decimal | date | datetime |  boolean`

This is a standard layout for a table metadata json. `$schema` points to another json that validates the structure of out table metadata files. Your table will have this `name` and `description` _(Note: It is strongly suggested that the name of your table matches the name of the metadata json file)_ when the database is created. The `location` is the relative folder path to where your table exists. This path is relative to your database `base_folder`. This means that the full path your table is `s3://<database.bucket>/<database.base_folder>/<table.folder>/`. So in this example the table employees should be in the s3 path `s3://my-bucket/database/database1/employees`. The `data_format` specifies what type of data the table is. Finally your columns is an array of objects. Where each object is a column definition specifying the `name`, `description` and `type` (data type) of the column. Each column can have optional arguments `pattern`, `enum` and `nullable` (see [table_schema.json](https://moj-analytical-services.github.io/metadata_schema/table/v1.0.0.json) for definition).

**Note:** that the order of the columns listed here should be the order of the columns in the table _(remember that data for a table should not have a header so the data will be queried wrong if the column order does not match up with the actual data)_.

Here is another table in the database called teams. The teams table is a list of employee IDs for each team. Showing which employees are in each team. This table is taken each month (so you can see which employee was in which team each month). Therefore this table is partitioned by each monthly snapshot.

```json
{
    "$schema" : "https://moj-analytical-services.github.io/metadata_schema/table/v1.0.0.json",
    "name": "teams",
    "description": "month snapshot of which employee with working in what team",
    "data_format": "parquet",
    "location": "teams/",
    "columns": [
        {
            "name": "team_id",
            "type": "int",
            "description": "ID given to each team",
            "nullable" : false
        },
        {
            "name": "team_name",
            "type": "character",
            "description": "name of the team"
        },
        {
            "name": "employee_id",
            "type": "int",
            "description": "primary key for each employee in the employees table",
            "pattern" : "\\d+"
        },
        {
            "name": "snapshot_year",
            "type": "int",
            "description": "year at which snapshot of workforce was taken"
        },
        {
            "name": "snapshot_month",
            "type": "int",
            "description": "month at which snapshot of workforce was taken",
            "enum" : [1,2,3,4,5,6,7,8,9,10,11,12]
        }
    ],
    "partitions" : ["snapshot_year", "snapshot_month"]
}
```

From the above you can see this has additional properties `enum`, `pattern`, `nullable` and a `partitions` property:

- **enum:** What values the column can take (does not have to include nulls - should use nullable property)
- **pattern:** Values in this column should match this regex string in the pattern property
- **nullable:** Specifies if this column should accept `NULL` values.
- **partitions:** Specifies if any of the columns in the table are file partitions rather than columns in the data. `etl_manager` will force your meta data json files to have columns that are partitions at the end of your data's list of columns.

> **Note:** etl_manager does not enforce information provided by `enum`, `pattern` and `nullable`. It is just there to provide information to other tools or functions that could use this information to validate your data. Also the information in `pattern`, `enum` and `nullable` can conflict etl_manager does not check for conflicts. For example a column with an enum of `[0,1]` and a pattern of `[A-Za-z]` is allowed.

### Examples using the DatabaseMeta Class

The easiest way to create a database is to run the code below. It reads a database schema based on the json files in a folder and creates this database meta in the glue catalogue. Allowing you to query the data using SQL using Athena.

```python
from etl_manager.meta import read_database_folder
db = read_database_folder('example_meta_data/')
db.create_glue_database()
```

The code snippet below creates a database meta object that allows you to manipulate the database and the tables that exist in it

```python
from etl_manager.meta import read_database_folder

db = read_database_folder('example_meta_data/')

# Database has callable objects

db.name # workforce

db.table_names # [employees, teams]

# Each table in the database is an object from the TableMeta Class which can be callable from the database meta object

db.table('employees').columns # returns all columns in employees table

# The db and table object properties can also be altered and updated

db.name = 'new_db_name'
db.name # 'new_db_name

db.table('employees').name = 'new_name'

db.table_names # [new_name, teams]

db.remove_table('new_name')

db.name # workforce_dev (note as default the package adds _dev if a db_suffix is not provided in DatabaseMeta)

# Set all table types to parquet and create database schema in glue
for t in db_table_names :
    db.table(t).data_format = 'parquet'
db.create_glue_database()
```

## Using the GlueJob Class

The GlueJob class can be used to run pyspark jobs on AWS Glue. It is worth keeping up to date with AWS release notes and general guidance on running Glue jobs. This class is a wrapper function to simplify running glue jobs by using a structured format.

```python
from etl_manager.etl import GlueJob

my_role = 'aws_role'
bucket = 'bucket-to-store-temp-glue-job-in'

job = GlueJob('glue_jobs/simple_etl_job/', bucket=bucket, job_role=my_role, job_arguments={"--test_arg" : 'some_string'})
job.run_job()

print(job.job_status)
```

### Glue Job Folder Structure

Glue jobs have the prescribed folder format as follows:

```
├── glue_jobs/
|   |
│   ├── job1/
|   |   ├── job.py
|   |   ├── glue_resources/
|   |   |   └── my_lookup_table.csv
|   |   └── glue_py_resources/
|   |       ├── my_python_functions.zip
|   |       └── github_zip_urls.txt
|   |   └── glue_jars/
|   |       └── my_jar.jar
|   |
│   ├── job2/
│   |   ├── job.py
│   |   ├── glue_resources/
│   |   └── glue_py_resources/
|   |
|   ├── shared_job_resources/
│   |   ├── glue_resources/
|   |   |   └── meta_data_dictionary.json
│   |   └── glue_py_resources/
|   |   └── glue_jars/
|   |       └── my_other_jar.jar
```

Every glue job folder must have a `job.py` script in that folder. That is the only required file everything else is optional. When you want to create a glue job object you point the GlueJob class to the parent folder of the `job.py` script you want to run. There are two additional folders you can add to this parent folder :

#### glue_resources folder

Any files in this folder are uploaded to the working directory of the glue job. This means in your `job.py` script you can get the path to these files by:

```python
path_to_file = os.path.join(os.getcwd(), 'file_in_folder.txt')
```

The GlueJob class will only upload files with extensions (.csv, .sql, .json, .txt) to S3 for the glue job to access.

#### glue_py_resources

These are python scripts you can import in your `job.py` script. e.g. if I had a `utils.py` script in my glue_py_resources folder. I could import that script normally e.g.

```python
from utils import *
```

You can also supply zip file which is a group of python functions in the standard python package structure. You can then reference this package as you would normally in python. For example if I had a package zipped as `my_package.zip` in the glue_py_resources folder then you could access that package normally in your job script like:

```python
from my_package.utils import *
```

You can also supply a text file with the special name `github_zip_urls.txt`. This is a text file where each line is a path to a github zip ball. The GlueJob class will download the github package rezip it and send it to S3. This github python package can then be accessed in the same way you would the local zip packages. For example if the `github_zip_urls.txt` file had a single line `https://github.com/moj-analytical-services/gluejobutils/archive/master.zip`. The package `gluejobutils` would be accessible in the `job.py` script:

```python
from gluejobutils.s3 import read_json_from_s3
```

#### shared_job_resources folder

This a specific folder (must have the name `shared_job_resources`). This folder has the same structure and restrictions as a normal glue job folder but does not have a `job.py` file. Instead anything in the `glue_resources` or `glue_py_resources` folders will also be used (and therefore uploaded to S3) by any other glue job. Take the example below:

```
├── glue_jobs/
│   ├── job1/
│   |   ├── job.py
│   |   ├── glue_resources/
|   |   |   └── lookup_table.csv
│   |   └── glue_py_resources/
|   |       └── job1_specific_functions.py
|   |
|   ├── shared_job_resources/
│   |   ├── glue_resources/
|   |   |   └── some_global_config.json
│   |   └── glue_py_resources/
|   |       └── utils.py
```

Running the glue job `job1` i.e.

```python
job = GlueJob('glue_jobs/job1/', bucket, job_role)
job.run_job()
```

This glue job would not only have access the the python script `job1_specific_functions.py` and file `lookup_table.csv` but also have access to the python script `utils.py` and file `some_global_config.json`. This is because the latter two files are in the `shared_job_resources` folder and accessible to all job folders (in their `glue_jobs` parent folder).

**Note:** Users should make sure there is no naming conflicts between filenames that are uploaded to S3 as they are sent to the same working folder.

### Using the Glue Job class

Returning to the initial example:

```python
from etl_manager.etl import GlueJob

my_role = 'aws_role'
bucket = 'bucket-to-store-temp-glue-job-in'

job = GlueJob('glue_jobs/simple_etl_job/', bucket=bucket, job_role=my_role)
```

Allows you to create a job object. The GlueJob class will have a `job_name` which is defaulted to the folder name you pointed it to i.e. in this instance the job is called `simple_etl_job`. To change the job name:

```python
job.job_name = 'new_job_name'
```

In AWS you can only have unique job names.

Other useful function and properties:

```python
# Increase the number of workers on a glue job (default is 2)
job.allocated_capacity = 5

# Set job arguments these are input params that can be accessed by the job.py script
job.job_arguments = {"--test_arg" : 'some_string', "--enable-metrics" : ""}
```

#### job_arguments

These are strings that can be passed to the glue job script. Below is an example of how these are accessed in the `job.py` script. This code snippit is taken from the `simple_etl_job` found in the `example` folder of this repo.

```python
# Example job tests access to all files passed to the job runner class
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils.s3 import read_json_from_s3

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'metadata_base_path', 'test_arg'])

print "JOB SPECS..."
print "JOB_NAME: ", args["JOB_NAME"]
print "test argument: ", args["test_arg"]

# Read in meta data json
meta_employees = read_json_from_s3(os.path.join(args['metadata_base_path'], "employees.json"))

### etc
```

>**Notes:**
> - The `test_arg` does not have two dashes in front of it. When specifying job_arguments with the GlueJob class it must be suffixed with `--` but you should remove these when accessing the args in the `job.py` script.
> - `metadata_base_path` is a special parameter that is set by the GlueJob class. It is the S3 path to where the `meta_data` folder is in S3 so that you can read in your agnostic metadata files if you want to use them in your glue job. Note that the [gluejobutils](https://github.com/moj-analytical-services/gluejobutils) package has a lot of functionality with integrating our metadata jsons with spark.
> - The GlueJob argument `--enable-metrics` is also a special parameter that enables you to see metrics of your glue job. [See here for more details on enabling metrics](https://docs.aws.amazon.com/en_us/glue/latest/dg/monitor-profile-glue-job-cloudwatch-metrics.html).
> - Note that `JOB_NAME` is a special parameter that is not set in GlueJob but automatically passed to the AWS Glue when running `job.py`. [See here for more on special parameters](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html).

Example of full `glue_job` and `meta_data` structures and code can be found [here](https://github.com/moj-analytical-services/etl_manager/tree/master/example).

# Unit Tests

This package has [unit tests](https://github.com/moj-analytical-services/etl_manager/blob/master/tests/test_tests.py) which can also be used to see functionality.

Unit tests can be ran by:

```python
python -m unittest tests.test_tests -v
```
