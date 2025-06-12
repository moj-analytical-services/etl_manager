import json
import os
import re
import urllib
import time
import pkg_resources
import jsonschema
import warnings
import copy

from etl_manager.utils import (
    read_json,
    write_json,
    _dict_merge,
    _end_with_slash,
    _validate_string,
    _validate_enum,
    _validate_pattern,
    _validate_nullable,
    _validate_sensitivity,
    _validate_redacted,
    _athena_client,
    _glue_client,
    _s3_resource,
    trim_complex_data_types,
    data_type_is_regex,
    s3_path_to_bucket_key,
)

_template = {
    "base": json.load(pkg_resources.resource_stream(__name__, "specs/base.json")),
    "avro": json.load(
        pkg_resources.resource_stream(__name__, "specs/avro_specific.json")
    ),
    "csv": json.load(
        pkg_resources.resource_stream(__name__, "specs/csv_specific.json")
    ),
    "csv_quoted_nodate": json.load(
        pkg_resources.resource_stream(__name__, "specs/csv_quoted_nodate_specific.json")
    ),
    "regex": json.load(
        pkg_resources.resource_stream(__name__, "specs/regex_specific.json")
    ),
    "orc": json.load(
        pkg_resources.resource_stream(__name__, "specs/orc_specific.json")
    ),
    "parquet": json.load(
        pkg_resources.resource_stream(__name__, "specs/parquet_specific.json")
    ),
    "json": json.load(
        pkg_resources.resource_stream(__name__, "specs/json_specific.json")
    ),
}

_agnostic_to_glue_spark_dict = json.load(
    pkg_resources.resource_stream(__name__, "specs/glue_spark_dict.json")
)

_web_link_to_table_json_schema = (
    "https://moj-analytical-services.github.io/metadata_schema/table/v1.4.0.json"
)

try:
    with urllib.request.urlopen(_web_link_to_table_json_schema, timeout=5) as url:
        _table_json_schema = json.loads(url.read().decode())
except urllib.error.URLError:
    warnings.warn(
        "Could not get schema from URL. Reading schema from package instead..."
    )
    _table_json_schema = json.load(
        pkg_resources.resource_stream(__name__, "specs/table_schema.json")
    )

_supported_column_types = _table_json_schema["properties"]["columns"]["items"][
    "properties"
]["type"]["enum"]
_supported_data_formats = _table_json_schema["properties"]["data_format"]["enum"]
_supported_column_sensitivity = _table_json_schema["properties"]["columns"]["items"][
    "properties"
]["sensitivity"]["enum"]
_column_properties = list(
    _table_json_schema["properties"]["columns"]["items"]["properties"].keys()
)


class MetaColumnTypeMismatch(Exception):
    pass


def _get_spec(spec_name):
    if spec_name not in _template:
        raise ValueError(
            f"spec_name/data_type requested ({spec_name}) is not a valid spec/data_type"
        )

    return copy.deepcopy(_template[spec_name])


class TableMeta:
    """
    Manipulate the agnostic metadata associated with a table and convert to a Glue spec
    """

    def __init__(
        self,
        name,
        location,
        columns=[],
        data_format="csv",
        description="",
        partitions=[],
        primary_key=[],
        glue_specific={},
        database=None,
    ):

        self.name = name
        self.location = location
        self.columns = copy.deepcopy(columns)
        self.data_format = data_format
        self.description = description
        self.partitions = copy.deepcopy(partitions)
        self.primary_key = copy.deepcopy(primary_key)
        self.glue_specific = copy.deepcopy(glue_specific)
        self.database = database

        self._update_sensitivity()

        self.validate_json_schema()
        self.validate_column_types()

    def validate_json_schema(self):
        jsonschema.validate(trim_complex_data_types(self.to_dict()), _table_json_schema)

    def validate_column_types(self):
        assert all(data_type_is_regex(c["type"]) for c in self.to_dict()["columns"])

    @property
    def name(self):
        return self._name

    # Adding validation as Athena doesn't like names with dashes
    @name.setter
    def name(self, name):
        _validate_string(name)
        self._name = name

    @property
    def data_format(self):
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._check_valid_data_format(data_format)
        self._data_format = data_format

    @property
    def column_names(self):
        return [c["name"] for c in self.columns]

    # partitions
    @property
    def partitions(self):
        return self._partitions

    @partitions.setter
    def partitions(self, partitions):
        if not partitions:
            self._partitions = []
        else:
            for p in partitions:
                self._check_column_exists(p)
            new_col_order = [c for c in self.column_names if c not in partitions]
            new_col_order = new_col_order + partitions
            self._partitions = partitions
            self.reorder_columns(new_col_order)

    @property
    def primary_key(self):
        return self._primary_key

    @primary_key.setter
    def primary_key(self, primary_key):
        if not (isinstance(primary_key, list) or primary_key is None):
            raise TypeError("primary_key must be type list or None")
        if not primary_key:
            self._primary_key = []
        else:
            for pk in primary_key:
                self._check_column_exists(pk)
            self._primary_key = primary_key

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        _validate_string(location, allowed_chars="_/-", allow_upper=True)
        if location and location != "":
            if location[0] == "/":
                raise ValueError("location should not start with a slash")
            self._location = location
        else:
            raise ValueError(
                "Your table must exist inside a folder in S3. "
                "Please specify a location."
            )

    @property
    def sensitivity(self):
        return self._sensitivity

    def _update_sensitivity(self):
        column_sensitivities = {
            column["sensitivity"] for column in self.columns if "sensitivity" in column
        }
        if column_sensitivities:
            self._sensitivity = sorted(list(column_sensitivities))
        else:
            self._sensitivity = []

    @property
    def database(self):
        """
        database object table relates to
        """
        return self._database

    @database.setter
    def database(self, database):
        if database and (not isinstance(database, DatabaseMeta)):
            raise ValueError(
                "database must be a database meta object from the DatabaseMeta class."
            )
        self._database = database

    def remove_column(self, column_name):
        self._check_column_exists(column_name)
        new_cols = [c for c in self.columns if c["name"] != column_name]
        new_partitions = [p for p in self.partitions if p != column_name]
        new_primary_key = [pk for pk in self.primary_key if pk != column_name]
        self.columns = new_cols
        self.partitions = new_partitions
        self.primary_key = new_primary_key
        self._update_sensitivity()

    def add_column(
        self,
        name,
        type,
        description,
        pattern=None,
        enum=None,
        nullable=None,
        sensitivity=None,
        redacted=None,
    ):
        self._check_column_does_not_exist(name)
        self._check_valid_datatype(type)
        _validate_string(name)
        cols = self.columns
        cols.append({"name": name, "type": type, "description": description})
        if enum:
            _validate_enum(enum)
            cols[-1]["enum"] = enum
        if pattern:
            _validate_pattern(pattern)
            cols[-1]["pattern"] = pattern
        if nullable is not None:
            _validate_nullable(nullable)
            cols[-1]["nullable"] = nullable
        if sensitivity is not None:
            self._check_valid_column_sensitivity(sensitivity)
            cols[-1]["sensitivity"] = sensitivity
        if redacted is not None:
            _validate_redacted(redacted)
            cols[-1]["redacted"] = redacted

        self.columns = cols

        # Reorder columns if partitions exist
        if self.partitions:
            new_col_order = [c for c in self.column_names if c not in self.partitions]
            new_col_order = new_col_order + copy.deepcopy(self.partitions)
            self.reorder_columns(new_col_order)

        self._update_sensitivity()

    def reorder_columns(self, column_name_order):
        for c in self.column_names:
            if c not in column_name_order:
                raise ValueError(
                    f"input column_name_order is missing column ({c}) in meta table"
                )
        self.columns = sorted(
            self.columns, key=lambda x: column_name_order.index(x["name"])
        )

    def generate_glue_columns(self, exclude_columns=[]):

        glue_columns = []
        for c in self.columns:
            if c["name"] not in exclude_columns:
                new_c = {}
                new_c["Name"] = c["name"]
                new_c["Comment"] = c["description"]

                b1 = c["type"].lower().startswith("array")
                b2 = c["type"].lower().startswith("struct")
                b3 = c["type"].lower().startswith("decimal")

                if b1 or b2:
                    # Replace agnostic meta type with Athena type anywhere in string
                    # (user provides agnostic types, but we need Athena/glue type)
                    this_type = c["type"]
                    for key in _agnostic_to_glue_spark_dict.keys():
                        replacement_type = _agnostic_to_glue_spark_dict[key]["glue"]
                        this_type = this_type.replace(key, replacement_type)
                    new_c["Type"] = this_type
                elif b3:
                    this_type = c["type"]
                    replacement_type = _agnostic_to_glue_spark_dict["decimal"]["glue"]
                    this_type = this_type.replace("decimal", replacement_type)
                    new_c["Type"] = this_type
                else:
                    new_c["Type"] = _agnostic_to_glue_spark_dict[c["type"]]["glue"]
                glue_columns.append(new_c)

        return glue_columns

    def _check_valid_data_format(self, data_format):
        if data_format not in _supported_data_formats:
            sdf = ", ".join(_supported_data_formats)
            raise ValueError(
                (
                    f"The data_format provided ({data_format}) "
                    f"must match the supported data_type names: {sdf}"
                )
            )

    def _check_valid_datatype(self, data_type):
        if not data_type_is_regex(data_type):
            scf = ", ".join(_supported_column_types)
            raise ValueError(
                (
                    f"The data_type provided must match the "
                    f"supported data_type names: {scf}"
                )
            )

    def _check_valid_column_sensitivity(self, sensitivity):
        _validate_sensitivity(sensitivity)
        if sensitivity not in _supported_column_sensitivity:
            ss = ", ".join(_supported_column_sensitivity)
            raise ValueError(
                f"The sensitivity provided must match the supported "
                f"sensitivity names: {ss}"
            )

    def _check_column_exists(self, column_name):
        if column_name not in self.column_names:
            cn = ", ".join(self.column_names)
            raise ValueError(
                (
                    f"The column name: {column_name} does not "
                    f"match those existing in meta: {cn}"
                )
            )

    def _check_column_does_not_exist(self, column_name):
        if column_name in self.column_names:
            raise ValueError(
                (
                    f"The column name provided ({column_name}) "
                    f"already exists table in meta."
                )
            )

    def update_column(self, column_name, **kwargs):

        if len([k for k in kwargs.keys() if k in _column_properties]) == 0:
            raise ValueError(
                (
                    f"one or more of the function inputs "
                    f"({', '.join(_column_properties)}) must be specified."
                )
            )

        self._check_column_exists(column_name)

        new_cols = []
        for c in self.columns:
            if c["name"] == column_name:

                if "name" in kwargs:
                    _validate_string(kwargs["name"], "_")
                    c["name"] = kwargs["name"]

                if "type" in kwargs:
                    self._check_valid_datatype(kwargs["type"])
                    c["type"] = kwargs["type"]

                if "description" in kwargs:
                    c["description"] = kwargs["description"]

                if "pattern" in kwargs:
                    _validate_pattern(kwargs["pattern"])
                    c["pattern"] = kwargs["pattern"]

                if "enum" in kwargs:
                    _validate_enum(kwargs["enum"])
                    c["enum"] = kwargs["enum"]

                if "nullable" in kwargs:
                    _validate_nullable(kwargs["nullable"])
                    c["nullable"] = kwargs["nullable"]

                if "sensitivity" in kwargs:
                    self._check_valid_column_sensitivity(kwargs["sensitivity"])
                    c["sensitivity"] = kwargs["sensitivity"]

                if "redacted" in kwargs:
                    _validate_redacted(kwargs["redacted"])
                    c["redacted"] = kwargs["redacted"]

            new_cols.append(c)

        self.columns = new_cols
        self._update_sensitivity()

    def glue_table_definition(self, full_database_path=None):

        glue_table_definition = _get_spec("base")
        specific = _get_spec(self.data_format)
        _dict_merge(glue_table_definition, specific)

        # Create glue specific variables from meta data
        glue_table_definition["Name"] = self.name
        glue_table_definition["Description"] = self.description

        glue_table_definition["StorageDescriptor"][
            "Columns"
        ] = self.generate_glue_columns(exclude_columns=self.partitions)

        if self.data_format == "json":
            non_partition_names = [
                c for c in self.column_names if c not in self.partitions
            ]
            glue_table_definition["StorageDescriptor"]["SerdeInfo"]["Parameters"][
                "paths"
            ] = ",".join(non_partition_names)

        if full_database_path:
            glue_table_definition["StorageDescriptor"]["Location"] = os.path.join(
                full_database_path, self.location
            )
        elif self.database:
            glue_table_definition["StorageDescriptor"]["Location"] = os.path.join(
                self.database.s3_database_path, self.location
            )
        else:
            raise ValueError(
                (
                    "Need to provide a database or full "
                    "database path to generate glue table def"
                )
            )

        if self.glue_specific:
            _dict_merge(glue_table_definition, self.glue_specific)

        if len(self.partitions) > 0:
            not_partitions = [c for c in self.column_names if c not in self.partitions]
            glue_partition_cols = self.generate_glue_columns(
                exclude_columns=not_partitions
            )

            glue_table_definition["PartitionKeys"] = glue_partition_cols

        return glue_table_definition

    def to_dict(self):
        meta = {
            "$schema": _web_link_to_table_json_schema,
            "name": self.name,
            "description": self.description,
            "data_format": self.data_format,
            "columns": self.columns,
            "location": self.location,
        }

        if bool(self.partitions):
            meta["partitions"] = self.partitions

        if bool(self.primary_key):
            meta["primary_key"] = self.primary_key

        if bool(self.glue_specific):
            meta["glue_specific"] = self.glue_specific

        return meta

    def write_to_json(self, file_path):
        write_json(self.to_dict(), file_path)

    def generate_markdown_doc(self, filepath):
        """
        write the table meta to a human readable markdown file
        """
        # def md_header(text, num) :
        #     return f"{'#'*num} {text}\n\n"

        if self.database:
            db_name = self.database.name
            full_s3_path = os.path.join(self.database.s3_database_path, self.location)
        else:
            db_name = "unknown"
            full_s3_path = "unknown"

        partition_text = ", ".join(self.partitions) if self.partitions else "None"

        primary_key_text = ", ".join(self.primary_key) if self.primary_key else "None"

        f = open(filepath, "w")
        f.write(f"# {self.name}")
        f.write(f"\n")
        f.write(
            (
                "*Note: This meta data document has been automatically "
                "generated by the etl_manager package*"
            )
        )
        f.write("\n")
        f.write("## Details")
        f.write("\n")
        f.write(f"**Description:** {self.description}")
        f.write("\n")
        f.write("\n")
        f.write(f"**Table Format:** {self.data_format}")
        f.write("\n")
        f.write("\n")
        f.write(f"**Table Partitions:** {partition_text}")
        f.write("\n")
        f.write("\n")
        f.write(f"**Primary Key:** {primary_key_text}")
        f.write("\n")
        f.write("\n")
        f.write(f"**Database Name:** {db_name}")
        f.write("\n")
        f.write("\n")
        f.write(f"**S3 Path:** {full_s3_path}")
        f.write("\n")
        f.write("## Table Columns")
        f.write("\n")
        f.write("***")
        f.write("\n")
        for c in self.columns:
            # f.write(f"**name:** {c['name']}")
            f.write(f"### {c['name']}")
            if c["name"] in self.partitions:
                f.write("\n")
                f.write(" *(partition)*")
            f.write("\n")
            f.write("\n")
            f.write(f"**type:** {c['type']}")
            f.write("\n")
            f.write("\n")
            f.write(f"**description:** {c['description']}")
            f.write("\n")
            f.write("***")
            f.write("\n")

    def refresh_partitions(
        self, temp_athena_staging_dir=None, database_name=None, timeout=None
    ):
        """
        Refresh the partitions in a table, if they exist
        """

        if self.partitions:
            if not temp_athena_staging_dir:
                if self.database:
                    temp_athena_staging_dir = self.database.s3_athena_temp_folder
                else:
                    raise ValueError(
                        (
                            f"You must provide a path to a directory "
                            f"in s3 for Athena to cache query results"
                        )
                    )

            if not database_name:
                if self.database:
                    database_name = self.database.name
                else:
                    raise KeyError(
                        (
                            "You must provide a database name, or register "
                            "a database object against the table"
                        )
                    )

            sql = f"MSCK REPAIR TABLE {database_name}.{self.name}"

            response = _athena_client.start_query_execution(
                QueryString=sql,
                ResultConfiguration={"OutputLocation": temp_athena_staging_dir},
            )

            sleep_time = 2
            counter = 0
            while True:
                athena_status = _athena_client.get_query_execution(
                    QueryExecutionId=response["QueryExecutionId"]
                )
                if athena_status["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
                    break
                elif athena_status["QueryExecution"]["Status"]["State"] in [
                    "QUEUED",
                    "RUNNING",
                ]:
                    time.sleep(sleep_time)
                elif athena_status["QueryExecution"]["Status"]["State"] == "FAILED":
                    raise ValueError(
                        "athena failed - response error:\n {}".format(
                            athena_status["QueryExecution"]["Status"][
                                "StateChangeReason"
                            ]
                        )
                    )
                else:
                    raise ValueError(
                        f"Athena failed - unknown reason (printing full response):\n "
                        f"{athena_status}"
                    )

                counter += 1
                if timeout:
                    if counter * sleep_time > timeout:
                        raise ValueError("athena timed out")

            return response


class DatabaseMeta:
    """
    Python class to manage glue databases from our agnostic meta data.

    db = DatabaseMeta('path_to_local_meta_data_folder/')

    This will create a database object that also holds table objects for each table
    json in the folder it is pointed to.

    The meta data folder used to initialise the database must contain a database.json
    file.
    """

    def __init__(self, name, bucket, base_folder="", description=""):

        self._tables = []
        self.name = name
        self.bucket = bucket
        self.base_folder = base_folder
        self.description = description

    @property
    def name(self):
        return self._name

    # Adding validation as Athena doesn't like names with dashes
    @name.setter
    def name(self, name):
        _validate_string(name)
        self._name = name

    @property
    def bucket(self):
        """
        The s3 bucket in which the database exists.
        """
        return self._bucket

    @bucket.setter
    def bucket(self, bucket):
        _validate_string(bucket, allowed_chars=".-")
        self._bucket = bucket

    @property
    def table_names(self):
        """
        Returns the names of the table objects in the database object.
        """
        table_names = [t.name for t in self._tables]
        return table_names

    @property
    def s3_database_path(self):
        """
        Returns the s3 path to the database
        """
        return os.path.join("s3://", self.bucket, self.base_folder)

    @property
    def s3_athena_temp_folder(self):
        """
        Athena needs to use a temporary bucket to run queries
        """
        return os.path.join("s3://", self.bucket, "__temp_athena__")

    def _check_table_exists(self, table_name):
        return table_name in self.table_names

    def _throw_error_check_table(self, table_name, error_on_table_exists=True):
        error_string = (
            f"Table {table_name} already exists."
            if error_on_table_exists
            else f"Table {table_name} does not exist."
        )
        if self._check_table_exists(table_name) == error_on_table_exists:
            raise ValueError(error_string)

    def table(self, table_name):
        """
        Returns table object that is in database object.
        table_name is the name of the table obj you want to return i.e. table.name.
        """
        self._throw_error_check_table(table_name, error_on_table_exists=False)
        out = [t for t in self._tables if t.name == table_name][0]
        return out

    def add_table(self, table):
        """
        Adds a table object to the database object.
        table must be of type TableMeta
        For example, table = TableMeta(example_meta_data/employees.json)
        """
        if not isinstance(table, TableMeta):
            raise ValueError("table must be of type TableMeta")
        self._throw_error_check_table(table.name)

        if not table.database:
            table.database = self

        self._tables.append(table)

    def remove_table(self, table_name):
        """
        Removes a Table object from the database object.
        table_name : name of the table object i.e. table.name
        """
        self._throw_error_check_table(table_name, False)
        self._tables = [t for t in self._tables if t.name != table_name]

    def delete_glue_database(self):
        """
        Deletes a glue database with the same name. Returns a response explaining if it
        was deleted or didn't delete because database was not found.
        """
        try:
            _glue_client.delete_database(Name=self.name)
            response = "database deleted"
        except _glue_client.exceptions.EntityNotFoundException:
            response = "database not found in glue catalogue"
            pass

        return response

    def delete_data_in_database(self, tables_only=False):
        """
        Deletes the data that is in the databases s3_database_path. If tables only is
        False, then the entire database folder is deleted otherwise the class will only
        delete folders corresponding to the tables in the database.
        """
        bucket = _s3_resource.Bucket(self.bucket)
        database_obj_folder = self.base_folder
        if tables_only:
            for t in self.table_names:
                # Need to end with a / to ensure we don't delete any filepaths that
                # match the same name
                table_s3_obj_folder = _end_with_slash(
                    os.path.join(database_obj_folder, self.table(t).location)
                )
                bucket.objects.filter(Prefix=table_s3_obj_folder).delete()
        else:
            database_obj_folder = (
                database_obj_folder
                if database_obj_folder == ""
                else _end_with_slash(database_obj_folder)
            )
            bucket.objects.filter(Prefix=database_obj_folder).delete()

    def create_glue_database(self, delete_if_exists=False):
        """
        Creates a database in Glue based on the database object calling the method
        function.

        By default, will error out if database exists - unless delete_if_exists is set
        to True (default is False).
        """
        db = {"DatabaseInput": {"Description": self.description, "Name": self.name}}

        if delete_if_exists:
            self.delete_glue_database()

        _glue_client.create_database(**db)

        for tab in self._tables:
            glue_table_def = tab.glue_table_definition(self.s3_database_path)
            _glue_client.create_table(DatabaseName=self.name, TableInput=glue_table_def)

    def update_glue_database(
        self, update_database_metadata=True, update_tables_if_exist=False
    ):

        """
        Updates a database in Glue based on the database object calling the method function.
        """
        db = {
            "Name": self.name,
            "DatabaseInput": {"Description": self.description, "Name": self.name},
        }

        if update_database_metadata:
            _glue_client.update_database(**db)

        for tab in self._tables:
            try:
                _glue_client.get_table(Name=tab.name, DatabaseName=self.name)
                table_exists = True
            except _glue_client.exceptions.EntityNotFoundException:
                table_exists = False

            if not table_exists:
                glue_table_def = tab.glue_table_definition(self.s3_database_path)
                _glue_client.create_table(
                    DatabaseName=self.name, TableInput=glue_table_def
                )

            if table_exists and update_tables_if_exist:
                glue_table_def = tab.glue_table_definition(self.s3_database_path)
                _glue_client.update_table(
                    DatabaseName=self.name, TableInput=glue_table_def
                )

    def to_dict(self):
        db_dict = {
            "description": self.description,
            "name": self.name,
            "bucket": self.bucket,
            "base_folder": self.base_folder,
        }
        return db_dict

    def write_to_json(self, folder_path, write_tables=True):
        """
        Writes the database object back into the agnostic meta data json files.

        Function writes a file called database.json to the folder_path provided.

        If write_tables is True (default) this method will also write all table objects
        as an agnostic meta data json.

        The table meta data json will be saved as <table_name>.json where
        table_name == table.name.
        """

        write_json(self.to_dict(), os.path.join(folder_path, "database.json"))

        if write_tables:
            for t in self._tables:
                t.write_to_json(os.path.join(folder_path, t.name + ".json"))

    def refresh_all_table_partitions(self):
        for table in self._tables:
            table.refresh_partitions()

    def test_column_types_align(self, exclude_tables=[]):
        """
        Tests if all column types across all tables in the database match.
        Add list of table names in exclude_tables to skip test on particular tables.
        """
        # Get all cols
        all_col_test = {}
        for t in self.table_names:
            if t not in exclude_tables:
                tb = self.table(t)
                for c in tb.columns:
                    if c["name"] in all_col_test:
                        all_col_test[c["name"]]["tables"].append(t)
                        all_col_test[c["name"]]["types"].add(c["type"])
                        all_col_test[c["name"]][
                            "traceback_log"
                        ] += f"===> {t}: {c['type']}\n"
                    else:
                        all_col_test[c["name"]] = {
                            "tables": [t],
                            "types": set([c["type"]]),
                            "traceback_log": f"===> {t}: {c['type']}\n",
                        }

        #  Run test
        failure = False
        error_log = ""
        for k in all_col_test.keys():
            if len(all_col_test[k]["types"]) > 1:
                error_log += (
                    f"ERROR: column {k} has multiple types "
                    f"[{', '.join(list(all_col_test[k]['types']))}]\n"
                    f"------------------\n{all_col_test[k]['traceback_log']}\n"
                )
                failure = True

        # Check results
        if failure:
            raise MetaColumnTypeMismatch(f"Meta data does not align...\n\n{error_log}")


##### END OF DATABASEMETA CLASS #####

# Create meta objects from json files or directories
def read_table_json(filepath, database=None):
    meta = read_json(filepath)
    if "partitions" not in meta:
        meta["partitions"] = []

    if "primary_key" not in meta:
        meta["primary_key"] = []

    if "glue_specific" not in meta:
        meta["glue_specific"] = {}

    tab = TableMeta(
        name=meta["name"],
        location=meta["location"],
        columns=meta["columns"],
        data_format=meta["data_format"],
        description=meta["description"],
        partitions=meta["partitions"],
        primary_key=meta["primary_key"],
        glue_specific=meta["glue_specific"],
        database=database,
    )

    return tab


def read_database_json(filepath):
    db_meta = read_json(filepath)
    db = DatabaseMeta(
        name=db_meta["name"],
        bucket=db_meta["bucket"],
        base_folder=db_meta["base_folder"],
        description=db_meta["description"],
    )
    return db


def read_database_folder(folderpath):
    # Always assigned to database through keyword argument
    db = read_database_json(os.path.join(folderpath, "database.json"))

    files = os.listdir(folderpath)
    files = set(
        [f for f in files if re.match(r".+\.json$", f) and f != "database.json"]
    )

    for f in files:
        table_file_path = os.path.join(folderpath, f)
        tm = read_table_json(table_file_path, database=db)
        db.add_table(tm)
    return db


def get_existing_database_from_glue_catalogue(database_name):
    """
    Find a database in the Glue catalogue from its database_name,
    and return a corresponding etl_manager DatabaseMeta object
    Note that the DatabaseMeta object will contain no tables
    i.e. it will NOT contain a list of tables already in the database.
    """
    glue_db = _glue_client.get_database(Name=database_name)
    database_description = glue_db["Database"]["Description"]
    tables = _glue_client.get_tables(DatabaseName=database_name)["TableList"]
    try:
        s3_path = tables[0]["StorageDescriptor"]["Location"]
    except IndexError:
        raise ValueError(
            (
                "There are no tables in this database, so there's not "
                "enough metadata to create an etl_manager db for you"
            )
        )
    bucket = s3_path_to_bucket_key(s3_path)[0]
    db = DatabaseMeta(
        name=database_name, bucket=bucket, description=database_description
    )
    return db


def _convert_etl_manager_agnostic_type_to_glue_type(type_string):
    # Convert etl_manager agnostic types to glue/athena datatypes
    for key, value in _agnostic_to_glue_spark_dict.items():
        type_string = type_string.replace(value["glue"], key)
    type_string = type_string.replace("integer", "int")
    return type_string


def _parquet_metadata_type_to_etl_mgr_type(pmeta_type):
    """
    Convert a field from parquet metadata dictionary to a etl_manager type string
    """

    if type(pmeta_type) == str:
        type_string = _convert_etl_manager_agnostic_type_to_glue_type(pmeta_type)

    # If it's not a string, it's a complex type
    elif pmeta_type["type"] == "struct":
        fields = pmeta_type["fields"]
        type_items = []
        for field in fields:
            key = field["name"]
            # each field can itself be a struct or array, so recurse
            etl_type = _parquet_metadata_type_to_etl_mgr_type(field["type"])
            type_items.append(f"{key}:{etl_type}")
        type_items_string = ",".join(type_items)
        type_string = f"struct<{type_items_string}>"

    elif pmeta_type["type"] == "array":
        element_type = pmeta_type["elementType"]
        # elements of an array can themselves be a struct or array, so recurse
        et_string = _parquet_metadata_type_to_etl_mgr_type(element_type)
        type_string = f"array<{et_string}>"

    return type_string


def tablemeta_from_parquet_meta(pmeta_json, name, location):
    """
    Create a TableMeta object from a parquet metadata dictionary
    i.e. the result of either:

    df = spark.read.parquet("path_to_parquet")
    pmeta_json = df.schema.json()

    or

    from pyarrow.parquet import ParquetFile
    md = ParquetFile("test_nest.parquet").metadata
    pmeta_json = md.metadata[b"org.apache.spark.sql.parquet.row.metadata"]
    """

    pmeta_dict = json.loads(pmeta_json)

    tab = TableMeta(name=name, location=location, data_format="parquet")
    for field in pmeta_dict["fields"]:
        data_type_string = _parquet_metadata_type_to_etl_mgr_type(field["type"])
        tab.add_column(field["name"], data_type_string, description="")

    return tab
