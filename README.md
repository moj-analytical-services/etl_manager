# mojdbtemplate

A python package to create a database on the platform using our moj data warehousing framework.

The main functionality of this package is to sync and run jobs on AWS.

To unit test the package

```python
python -m unittest tests.test_tests -v
```

## Rules

- Package does not need to be able to run on glue. Therefore python 3 is fine.
- Work from folder e.g. v1
- Running on dev is default
- Only the code specifies the dev - you never have meta data or folders renamed locally or on github
- Every function needs to be unit tested
- Use data engineering warehouse template as unit tests for functions
- All of the data dependencies of the job should be ran from s3. Even if the job is ran on python locally code should download data from s3, process it and upload to s3


## Examples

### Agnostic Meta Data

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
    "bucket": "my_bucket",
    "base_folder": "my_folder/",
    "location": "database/database1/"
}
```

The employees table has an ID for each employee their name and dob. The table meta looks like this:

```json
{
    "id": "workforce.employees",
    "table_name": "employees",
    "table_desc": "table containing employee information",
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

The teams table is a list of employee IDs for each team. Showing which employees are in each team. This table is taken each month (so you can see which employee was in which team each month). Therefore this table is partitioned by each monthly snapshot.

```json
{
    "id": "workforce.teams",
    "table_name": "teams",
    "table_desc": "month snapshot of which employee with working in what team",
    "data_format": "parquet",
    "location": "teams/",
    "columns": [
        {
            "name": "team_id",
            "type": "int",
            "description": "ID given to each team"
        },
        {
            "name": "team_name",
            "type": "character",
            "description": "name of the team"
        },
        {
            "name": "employee_id",
            "type": "int",
            "description": "primary key for each employee in the employees table"
        },
        {
            "name": "snapshot_year",
            "type": "int",
            "description": "year at which snapshot of workforce was taken"
        },
        {
            "name": "snapshot_month",
            "type": "int",
            "description": "month at which snapshot of workforce was taken"
        }
    ],
    "partitions" : ["snapshot_year", "snapshot_month"]
}
```

### Using the DatabaseMeta Class

The code snippet below creates a database meta object that allows you to manipulate the database and the tables that exist in it

```python
from mojdbtemplate.meta import DatabaseMeta

db = DatabaseMeta('example_meta_data/')

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

db.glue_name # workforce_dev (note as default the package adds _dev if a db_suffix is not provided in DatabaseMeta)

# Set all table types to parquet and create database schema in glue
for t in db_table_names :
    db.table(t).data_format = 'parquet'
db.create_glue_database()
```