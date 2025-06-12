# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## v9.0.0

### Change

- Upgrade setuptools
- Remove Python 3.8 support (EOL).

## v8.1.0

### Change

- Add timeout to the urllib request for fetching the metadata json file from github

## v8.0.0

### Change

- Add support for WorkerType and NumberOfWorkers (removing AllocatedCapacity - hence breaking change)

## v7.6.0

### Change

- Add support for AWS Glue v4.0

## v7.5.3

### Change

- Updated dependencies

## v7.5.2

### Change
- Added support for --additional-python-modules

## v7.5.1

### Change
- Fix Glue version setter
- Add support for Glue version 3.0

## v7.5.0

### Change
- Allow tags to be passed to Glue jobs

## v7.4.0

### Change
- Add support for `binary` type

## v7.3.0

### Change
- Set the default AWS Glue version to 2.0

## v7.2.0

### Change
- Added `partitions` and `primary_key` properties to table metadata

## v7.1.1

### Change
- Update CHANGELOG and pyproject.toml missed in release v7.1.0

## v7.1.0

### Change
- Allow use of decimal data type

## v7.0.6

### Change

- Added `sensitivity` and `redacted` properties to column metadata
- Added `sensitivity` property to table metadata

## v7.0.5

### Change

- Added the ability to automatically generate a TableMeta object from parquet metadata, using `tablemeta_from_parquet_meta`

## v7.0.4

### Change

- Added the ability to update an existing database with new tables - see `` and `meta.get_existing_database_from_glue_catalogue` and `DatabaseMeta.update_glue_database`
- Fixed bug that meant the use of complex types (arrays and structs) didn't actually work in Athena


## v7.0.3

### Change

- Users can now include jars in a `glue_jars` folder, and they will be uploaded to s3 and made available in the glue environment

## v7.0.2

### Change

- GlueJob now sets a timeout parameter for glue jobs. This can set to specific times (in minutes) using the `timeout_override_minutes` property
- Relaxed package requirements on jsonschema
- Removed `requirements.txt` as no longer used

## v7.0.1

### Change

- Removing validator from column description as it was too strict.

## v7.0.0

### Change

- ETL Manager now points to a web schema for tables (will get schema from package if cannot access schema web link - but will output warning)
- Updated package setup to `pyproject.toml`
- Replaced travis for github actions

## v6.0.0

### Change

- Glue jobs now run using Python 3 and Spark 2.4 as default

## v5.0.0

### Added

- ETL manager now allows use of STRUCT and ARRAY col types in your hive metadata tables.

## v4.0.0

### Updated

- Method function in TableMeta `refresh_paritions` renamed to `refresh_partitions`.
- `refresh_partitions` function now wait for athena to complete the query. This should avoid errors where you hit limits of concurrent Athena queries (max 4) when using `refresh_all_table_partitions` (from DatabaseMeta class).

## v3.1.0

### Added

- Two new input arguments to GlueJob method function `wait_for_completion`.
  - Input `back_off_retries` now is the number of retries to boto API to avoid Throttling Error. Retries are done with exponential back off.
  - `cleanup_if_successful` will delete the glue job if the `wait_for_completion` doesn't raise an error. i.e. Glue job completes successfully.

## v3.0.0

### Added

- Fixed issue [91](https://github.com/moj-analytical-services/etl_manager/issues/91) and [92](https://github.com/moj-analytical-services/etl_manager/issues/92)
- Improved python format
- Refactored to Python 3.6
- Fixed unknown issue where arguments passed into function were not copied (same memory location)

## v2.2.0

### Added

- Added argument `wait_seconds` to `GlueJob` class function `wait_for_job_completion()` to set number of seconds between job status checks. Default unchanged.

## v2.2.1

### Change

- Updated output from `GlueJob` class function `wait_for_job_completion()` (when verbose is set to True), now states how long Glue has been running the job.

## v2.2.0

### Added

- Fixed bug where glue_specific would not write to json or be a key in dictionary from TableMeta class `to_dict()` method.
- Fixed bug where default table ddl templates would be overwritten causing mixed table definitions (see issue no. 80) for specific example and fix.
- If meta has partition property if none or empty list then this property will no longer be passed to dict (and therefore not to json)
- If meta has glue_specific property if none or empty dict then this property will no longer be passed to dict (and therefore not to json)

## v2.1.2

### Added

- DatabaseMeta method function `test_column_types_align` now tests that all column types match across all tables in database object.

## v2.1.1

### Fix

- bug meant that new nullable column property was only being set if nullable was True.

## v2.1.0

### Change

- now allows newline json files as athena compatable tables (note still does not support struct or array column types - still on the todo list)
- Improved `delete_glue_database` method function to only catch/allow specific error (database does not exist)

## v2.0.0

### Change

- Meta data cols now has `enum`, `pattern` and `nullable` properties
- `wait_for_completition` method function now has verbose input param that prints out status with time stamp everytime boto checks on the glue job
- `update_column` method function of `TableMeta` class now takes kwargs that match the properties of the column. (Input params of `new_type`, `new_name`, etc will no longer work). e.g. new functionality works as `tab.update_column('col1', type = 'int')`.

## v1.0.5 - 2018-10-10

### Change

- Changed back end execution of `MSK REPAIR TABLE` call to athena. Have moved from `pyathenajdbc` to `boto3` to reduce number of package dependencies. etl_manager no longer requires `pyathenajdbc` (which also means do not need Java installed).

## v1.0.4 - 2018-09-17

### Change

- removed check that throws error for `-` in job parameter name due to the new Glue parameter `enable-metrics`

## v1.0.3 - 2018-09-20

### Change
- `--conf` allowed as job param to enable spark configuration for AWS Glue

## v1.0.2 - 2018-08-30

### Change

- Database meta class will now throw error if database already exists when calling create_glue_database

## v1.0.1 - 2018-08-30

### Added

- setup.py now installs package dependencies

## v1.0.0 - 2018-08-30

### Changed

- wait_for_completion method in GlueJob class now raises error if glue job was manually stopped
- updated setup.py to match github version

## v0.1.0 - 2018-08-23

### Added

- Initial release
