# etl_manager

[![Build Status](https://travis-ci.org/moj-analytical-services/etl_manager.svg?branch=master)](https://travis-ci.org/moj-analytical-services/etl_manager)

A python package that manages our data engineering framework and implements them on AWS Glue.

The main functionality of this package is to interact with AWS Glue to create meta data catalogues and run Glue jobs.

To install:

```bash
pip install git+git://github.com/moj-analytical-services/etl_manager.git#egg=etl_manager
```

If you do not have it installed already you will also need to install boto3.

**Currently supported data types for your columns currently are:**

`character | int | long | float | double | date | datetime |  boolean`

## Unit Tests

This package has [unit tests](https://github.com/moj-analytical-services/etl_manager/blob/master/tests/test_tests.py) which can also be used to see functionality.

Unit tests can be ran by:

```python
python -m unittest tests.test_tests -v
```

## Examples

### Basic Use

Let's say I have a single table (a csv file) and I want to query it using Amazon athena. My csv file is in the following S3 path: `s3://my-bucket/my-table-folder/file.csv`.

file.csv is a table that looks like this:

| col1 | col2 |
|------|------|
| a    | 1    |
| b    | 12   |
| c    | 42   |

As you can see col1 is a string and col2 is a integer.

#### Notes:

- for Athena to work your table should not contain a header. So before file.csv is uploaded to S3 you should make sure it has no header.
- Tables must be in a folder. I.e. the location of your table (`table.location`) should be the parent folder of where you data exists. See example below.

To create a schema for your data to be queried by Athena you can use the following code:

#### Basic Example

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
    "bucket": "my-bucket",
    "base_folder": "database/database1"
}
```

When you create your database it will have this `name` and `description`. The `bucket` key specifies where the database exists (and therefore where the tables exist) in S3. The `base_folder` is is the initial path to where the tables exist. If your tables are in folders directly in the bucket (e.g. `s3://my-bucket/table1/`) then you can leave base_folder as an empty string (`""`).

The employees table has an ID for each employee their name and dob. The table meta looks like this:

```json
{
    "$schema" : "https://raw.githubusercontent.com/moj-analytical-services/etl_manager/master/etl_manager/specs/table_schema.json",
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

This is a standard layout for a table metadata json. `$schema` points to another json that validates the structure of out table metadata files. Your table will have this `name` and `description` _(Note: It is strongly suggested that the name of your table matches the name of the metadata json file)_ when the database is created. The `location` is the relative folder path to where your table exists. This path is relative to your database `base_folder`. This means that the full path your table is `s3://<database.bucket>/<database.base_folder>/<table.folder>/`. So in this example the table employees should be in the s3 path `s3://my-bucket/database/database1/employees`. The `data_format` specifies what type of data the table is. Finally your columns is an array of objects. Where each object is a column definition specifying the `name`, `description` and `type` (data type) of the column. Note that the order of the columns listed here should be the order of the columns in the table _(remember that data for a table should not have a header so the data will be queried wrong if the column order does not match up with the actual data)_.

Here is another table in the database called teams. The teams table is a list of employee IDs for each team. Showing which employees are in each team. This table is taken each month (so you can see which employee was in which team each month). Therefore this table is partitioned by each monthly snapshot.

```json
{
    "$schema" : "https://raw.githubusercontent.com/moj-analytical-services/etl_manager/master/etl_manager/specs/table_schema.json",
    "name": "teams",
    "description": "month snapshot of which employee with working in what team",
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