from etl_manager.utils import read_json, write_json, _dict_merge, _end_with_slash, _validate_string, _glue_client, _s3_resource, _remove_final_slash
from copy import copy
import string
import json
import os
import re
import pkg_resources
from pyathenajdbc import connect
import jsonschema

_template = {
    "base":  json.load(pkg_resources.resource_stream(__name__, "specs/base.json")),
    "avro":  json.load(pkg_resources.resource_stream(__name__, "specs/avro_specific.json")),
    "csv":  json.load(pkg_resources.resource_stream(__name__, "specs/csv_specific.json")),
    "csv_quoted_nodate":  json.load(pkg_resources.resource_stream(__name__, "specs/csv_quoted_nodate_specific.json")),
    "regex":  json.load(pkg_resources.resource_stream(__name__, "specs/regex_specific.json")),
    "orc":  json.load(pkg_resources.resource_stream(__name__, "specs/orc_specific.json")),
    "par":  json.load(pkg_resources.resource_stream(__name__, "specs/par_specific.json")),
    "parquet":  json.load(pkg_resources.resource_stream(__name__, "specs/par_specific.json"))
}

_agnostic_to_glue_spark_dict = json.load(pkg_resources.resource_stream(__name__, "specs/glue_spark_dict.json"))
_table_json_schema = json.load(pkg_resources.resource_stream(__name__, "specs/table_schema.json"))
_web_link_to_table_json_schema = "https://raw.githubusercontent.com/moj-analytical-services/etl_manager/master/etl_manager/specs/table_schema.json"

_supported_column_types = _table_json_schema['properties']['columns']['items']['properties']["type"]["enum"]
_supported_data_formats = _table_json_schema['properties']['data_format']["enum"]

def _get_spec(spec_name) :
    if spec_name not in _template :
        raise ValueError("spec_name/data_type requested ({}) is not a valid spec/data_type".format(spec_name))

    return copy(_template[spec_name])

class TableMeta :
    """
    Manipulate the agnostic metadata associated with a table and convert to a Glue spec
    """

    def __init__(self, name, location, columns = [], data_format = 'csv',  description = '', partitions = [], glue_specific = {}, database = None) :
       
        self.name = name
        self.location = location
        self.columns = columns
        self.data_format = data_format
        self.description = description
        self.partitions = partitions
        self.glue_specific = glue_specific
        self.database = database

        jsonschema.validate(self.to_dict(), _table_json_schema)

    @property
    def name(self) :
        return self._name
    
    # Adding validation as Athena doesn't like names with dashes
    @name.setter
    def name(self, name) :
        _validate_string(name)
        self._name = name

    @property 
    def data_format(self) :
        return self._data_format
    
    @data_format.setter
    def data_format(self, data_format) :
        self._check_valid_data_format(data_format)
        self._data_format = data_format
    
    @property
    def column_names(self) :
        return [c['name'] for c in self.columns]

    # partitions
    @property
    def partitions(self) :
        return self._partitions

    @partitions.setter
    def partitions(self, partitions) :
        if partitions is None :
            self._partitions = []
        else :
            for p in partitions : self._check_column_exists(p)
            new_col_order = [c for c in self.column_names if c not in partitions]
            new_col_order = new_col_order + partitions
            self._partitions = partitions
            self.reorder_columns(new_col_order)

    @property
    def location(self) :
        return self._location

    @location.setter
    def location(self, location) :
        _validate_string(location, allowed_chars="_/-")
        if location and location != '' :
            if location[0] == '/':
                raise ValueError("location should not start with a slash")
            self._location = location
        else :
            raise ValueError("Your table must exist inside a folder in S3. Please specify a location.")

    @property
    def database(self):
        """
        database object table relates to
        """
        return self._database

    @database.setter
    def database(self, database) :
        if database and (not isinstance(database, DatabaseMeta)) :
            raise ValueError('database must be a database meta object from the DatabaseMeta class.')
        self._database = database

    def remove_column(self, column_name) :
        self._check_column_exists(column_name)
        new_cols = [c for c in self.columns if c['name'] != column_name]
        new_partitions = [p for p in self.partitions if p != column_name]
        self.columns = new_cols
        self.partitions = new_partitions

    def add_column(self, name, type, description) :
        self._check_column_does_not_exists(name)
        self._check_valid_datatype(type)
        _validate_string(name)
        cols = self.columns
        cols.append({"name": name, "type": type, "description": description})
        self.columns = cols

    def reorder_columns(self, column_name_order) :
        for c in self.column_names :
            if c not in column_name_order :
                raise ValueError("input column_name_order is missing column ({}) in meta table".format(c))
        self.columns = sorted(self.columns, key=lambda x: column_name_order.index(x['name']))

    def generate_glue_columns(self, exclude_columns = []) :

        glue_columns = []
        for c in self.columns :
            if c['name'] not in exclude_columns :
                new_c = {}
                new_c["Name"] = c["name"]
                new_c["Comment"] = c["description"]
                new_c["Type"] = _agnostic_to_glue_spark_dict[c['type']]['glue']
                glue_columns.append(new_c)

        return glue_columns

    def _check_valid_data_format(self, data_format) :
        if data_format not in _supported_data_formats :
            raise ValueError("The data_format provided ({}) must match the supported data_type names: {}".format(data_format, ", ".join(_supported_data_formats)))

    def _check_valid_datatype(self, data_type) :
        if data_type not in _supported_column_types :
            raise ValueError("The data_type provided must match the supported data_type names: {}".format(", ".join(_supported_column_types)))

    def _check_column_exists(self, column_name) :
        if column_name not in self.column_names :
            raise ValueError("The column name: {} does not match those existing in meta: {}".format(column_name, ", ".join(self.column_names)))

    def _check_column_does_not_exists(self, column_name) :
        if column_name in self.column_names :
            raise ValueError("The column name provided ({}) already exists table in meta.".format(column_name))

    def update_column(self, column_name, new_name = None, new_type = None, new_description = None) :

        self._check_column_exists(column_name)

        if new_name is None and new_type is None and new_description is None :
            raise ValueError("one or more of the function inputs (new_name, new_type and new_description) must be specified.")
        new_cols = []
        for c in self.columns :
            if c['name'] == column_name :

                if new_name is not None :
                    _validate_string(new_name, "_")
                    c['name'] = new_name

                if new_type is not None :
                    self._check_valid_datatype(new_type)
                    c['type'] = new_type

                if new_description is not None :
                    _validate_string(new_description, "_,.")
                    c['description'] = new_description

            new_cols.append(c)

        self.columns = new_cols

    def glue_table_definition(self, full_database_path = None) :

        glue_table_definition = _get_spec('base')
        specific = _get_spec(self.data_format)
        _dict_merge(glue_table_definition, specific)

        # Create glue specific variables from meta data
        glue_table_definition["Name"] = self.name
        glue_table_definition["Description"] = self.description

        glue_table_definition['StorageDescriptor']['Columns'] = self.generate_glue_columns(exclude_columns = self.partitions)

        if full_database_path:
            glue_table_definition['StorageDescriptor']["Location"] = os.path.join(full_database_path, self.location)
        elif self.database:
            glue_table_definition['StorageDescriptor']["Location"] = os.path.join(self.database.s3_database_path, self.location)
        else:
            raise ValueError("Need to provide a database or full database path to generate glue table def")

        if self.glue_specific:
            _dict_merge(glue_table_definition, self.glue_specific)

        if len(self.partitions) > 0 :
            not_partitions = [c for c in self.column_names if c not in self.partitions]
            glue_partition_cols = self.generate_glue_columns(exclude_columns = not_partitions)

            glue_table_definition['PartitionKeys'] = glue_partition_cols

        return glue_table_definition

    def to_dict(self) :
        meta = {
            "$schema": _web_link_to_table_json_schema,
            "name" : self.name,
            "description" : self.description,
            "data_format" : self.data_format,
            "columns" : self.columns,
            "partitions" : self.partitions,
            "location" : self.location
        }
        return meta

    def write_to_json(self, file_path) :
        write_json(self.to_dict(), file_path)

    def generate_markdown_doc(self, filepath) :
        """
        write the table meta to a human readable markdown file
        """
        # def md_header(text, num) :
        #     return f"{'#'*num} {text}\n\n"

        if self.database :
            db_name = self.database.name
            full_s3_path = os.path.join(self.database.s3_database_path, self.location)
        else :
            db_name = 'unknown'
            full_s3_path = 'unknown'

        partition_text = ', '.join(self.partitions) if self.partitions else 'None'
        
        f = open(filepath, "w")
        f.write(f"# {self.name}")
        f.write(f"\n")
        f.write("*Note: This meta data document has been automatically generated by the etl_manager package*")
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
        f.write(f"**Database Name:** {db_name}")
        f.write("\n")
        f.write("\n")
        f.write(f"**S3 Path:** {full_s3_path}")
        f.write("\n")
        f.write("## Table Columns")
        f.write("\n")
        f.write("***")
        f.write("\n")
        for c in self.columns :
            # f.write(f"**name:** {c['name']}")
            f.write(f"### {c['name']}")
            if c['name'] in self.partitions :
                f.write('\n')
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
        
        
    def refresh_paritions(self, temp_athena_staging_dir = None, database_name = None) :
        """
        Refresh the partitions in a table, if they exist
        """

        if self.partitions:
            if not temp_athena_staging_dir:
                if self.database:
                    temp_athena_staging_dir = self.database.s3_athena_temp_folder
                else:
                    raise ValueError("You must provide a path to a directory in s3 for Athena to cache query results")

            conn = connect(s3_staging_dir = temp_athena_staging_dir, region_name = 'eu-west-1')

            if not database_name:
                if self.database:
                    database_name = self.database.name
                else:
                    raise KeyError("You must provide a database name, or register a database object against the table")

            sql = "MSCK REPAIR TABLE {}.{}".format(database_name, self.name)

            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
            finally:
                conn.close()


class DatabaseMeta :
    """
    Python class to manage glue databases from our agnostic meta data.
    db = DatabaseMeta('path_to_local_meta_data_folder/')
    This will create a database object that also holds table objects for each table json in the folder it is pointed to.
    The meta data folder used to initialise the database must contain a database.json file.
    """
    def __init__(self, name, bucket, base_folder = '', description = '') :

        self._tables = []
        self.name = name
        self.bucket = bucket
        self.base_folder = base_folder
        self.description = description

    @property
    def name(self) :
        return self._name
    
    # Adding validation as Athena doesn't like names with dashes
    @name.setter
    def name(self, name) :
        _validate_string(name)
        self._name = name

    @property
    def bucket(self):
        """
        The s3 bucket in which the database exists.
        """
        return self._bucket

    @bucket.setter
    def bucket(self, bucket) :
        _validate_string(bucket, allowed_chars='.-')
        self._bucket = bucket

    @property
    def table_names(self) :
        """
        Returns the names of the table objects in the database object.
        """
        table_names = [t.name for t in self._tables]
        return table_names

    @property
    def s3_database_path(self) :
        """
        Returns the s3 path to the database
        """
        return os.path.join('s3://', self.bucket, self.base_folder)

    @property
    def s3_athena_temp_folder(self) :
        """
        Athena needs to use a temporary bucket to run queries
        """
        return os.path.join('s3://',self.bucket, "__temp_athena__")

    def _check_table_exists(self, table_name) :
        return table_name in self.table_names

    def _throw_error_check_table(self, table_name, error_on_table_exists = True) :
        error_string = "Table {} already exists.".format(table_name) if error_on_table_exists else "Table {} does not exist.".format(table_name)
        if self._check_table_exists(table_name) == error_on_table_exists:
            raise ValueError(error_string)

    def table(self, table_name) :
        """
        Returns table object that is in database object.
        table_name is the name of the table obj you want to return i.e. table.name.
        """
        self._throw_error_check_table(table_name, error_on_table_exists = False)
        out = [t for t in self._tables if t.name == table_name][0]
        return out

    def add_table(self, table) :
        """
        Adds a table object to the database object.
        table must be a table object e.g. table = TableMeta(example_meta_data/employees.json)
        """
        if not isinstance(table, TableMeta) :
            raise ValueError("table must an object of TableMeta class")
        self._throw_error_check_table(table.name)

        if not table.database:
            table.database = self

        self._tables.append(table)

    def remove_table(self, table_name) :
        """
        Removes a Table object from the database object.
        table_name : name of the table object i.e. table.name
        """
        self._throw_error_check_table(table_name, False)
        self._tables = [t for t in self._tables if t.name != table_name]

    def delete_glue_database(self) :
        """
        Deletes a glue database with the same name. Returns a response explaining if it was deleted or didn't delete because database was not found.
        """
        try :
            _glue_client.delete_database(Name = self.name)
            response = 'database deleted'
        except :
            response = 'Cannot delete as database not found in glue catalogue'
        return response

    def delete_data_in_database(self, tables_only = False) :
        """
        Deletes the data that is in the databases s3_database_path. If tables only is False, then the entire database folder is deleted otherwise the class will only delete folders corresponding to the tables in the database.
        """
        bucket = _s3_resource.Bucket(self.bucket)
        database_obj_folder = self.base_folder
        if tables_only :
            for t in self.table_names :
                # Need to end with a / to ensure we don't delete any filepaths that match the same name
                table_s3_obj_folder = _end_with_slash(os.path.join(database_obj_folder, self.table(t).location))
                bucket.objects.filter(Prefix=table_s3_obj_folder).delete()
        else :
            database_obj_folder = database_obj_folder if database_obj_folder == '' else _end_with_slash(database_obj_folder)
            bucket.objects.filter(Prefix=database_obj_folder).delete()

    def create_glue_database(self) :
        """
        Creates a database in Glue based on the database object calling the method function. If a database with the same name (db.name) already exists it overwrites it.
        """
        db = {
            "DatabaseInput": {
                "Description": self.description,
                "Name": self.name,
            }
        }

        del_resp = self.delete_glue_database()

        _glue_client.create_database(**db)

        for tab in self._tables :
            glue_table_def = tab.glue_table_definition(self.s3_database_path)
            _glue_client.create_table(DatabaseName = self.name, TableInput = glue_table_def)

    def to_dict(self) :
        db_dict = {
            "description": self.description,
            "name": self.name,
            "bucket": self.bucket,
            "base_folder": self.base_folder
        }
        return db_dict

    def write_to_json(self, folder_path, write_tables = True) :
        """
        Writes the database object back into the agnostic meta data json files.
        Function writes a file called database.json to the folder_path provided.
        If write_tables is True (default) this method will also write all table objects as an agnostic meta data json.
        The table meta data json will be saved as <table_name>.json where table_name == table.name.
        """

        write_json(self.to_dict(), os.path.join(folder_path, 'database.json'))

        if write_tables :
            for t in self._tables :
                t.write_to_json(os.path.join(folder_path, t.name + '.json'))

    def refresh_all_table_partitions(self):
        for table in self._tables:
                table.refresh_paritions()

# Create meta objects from json files or directories
def read_table_json(filepath, database = None) :
    meta = read_json(filepath)
    if 'partitions' not in meta :
        meta['partitions'] = []

    if "glue_specific" not in meta:
        meta['glue_specific'] = {}

    tab = TableMeta(name = meta['name'],
        location=meta['location'],
        columns=meta['columns'],
        data_format=meta['data_format'],
        description=meta['description'],
        partitions=meta['partitions'],
        glue_specific=meta['glue_specific'],
        database=database)
    
    return tab

def read_database_json(filepath) :
    db_meta = read_json(filepath)
    db = DatabaseMeta(name=db_meta['name'], bucket=db_meta['bucket'], base_folder=db_meta['base_folder'], description=db_meta['description'])
    return db

def read_database_folder(folderpath) :
    # Always assigned to database through keyword argument
    db = read_database_json(os.path.join(folderpath, 'database.json'))

    files = os.listdir(folderpath)
    files = set([f for f in files if re.match(".+\.json$", f) and f != 'database.json'])

    for f in files :
        table_file_path = os.path.join(folderpath, f)
        tm = read_table_json(table_file_path, database=db)
        db.add_table(tm)
    return db
