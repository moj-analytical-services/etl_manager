# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
