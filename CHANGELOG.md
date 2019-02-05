# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## v2.2.2
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
