# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

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
