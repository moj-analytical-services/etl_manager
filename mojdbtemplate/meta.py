from dataengineeringutils.utils import read_json, write_json, dict_merge
from copy import copy
import string
import json
import os
import re
import boto3
import pkg_resources

_glue_client = boto3.client('glue', 'eu-west-1')

_conversion = {
    "base": pkg_resources.resource_stream(__name__, "specs/base.json"),
    "avro": pkg_resources.resource_stream(__name__, "specs/avro_specific.json"),
    "csv": pkg_resources.resource_stream(__name__, "specs/csv_specific.json"),
    "csv_quoted_nodate": pkg_resources.resource_stream(__name__, "specs/csv_quoted_nodate_specific.json"),
    "regex": pkg_resources.resource_stream(__name__, "specs/regex_specific.json"),
    "orc": pkg_resources.resource_stream(__name__, "specs/orc_specific.json"),
    "par": pkg_resources.resource_stream(__name__, "specs/par_specific.json"),
    "parquet": pkg_resources.resource_stream(__name__, "specs/par_specific.json")
}

def _get_spec(spec_name) :
    if spec_name not in _conversion :
        raise ValueError("spec_name/data_type requested ({}) is not a valid spec/data_type".format(spec_name))

    return json.load(_conversion[spec_name])

# Used by both classes (Should move into another module)
def _validate_string(s, allowed_chars = "_") :
    if s != s.lower() :
        raise ValueError("string provided must be lowercase")
    
    invalid_chars = string.punctuation

    for a in allowed_chars :
        invalid_chars = invalid_chars.replace(a, "")
    
    if any(char in invalid_chars for char in s) :
        raise ValueError("punctuation excluding ({}) is not allowed in string".format(allowed_chars))

class Table_Meta :

    _supported_column_types = ('int', 'character', 'float', 'date', 'datetime', 'boolean', 'long')
    _supported_data_formats = ('avro', 'csv', 'csv_quoted_nodate', 'regex', 'orc', 'par', 'parquet')
    
    _agnostic_to_glue_spark_dict = {
        'character' : {'glue' : 'string', 'spark': 'StringType'},
        'int' : {'glue' : 'int', 'spark': 'IntegerType'},
        'long' : {'glue' : 'bigint', 'spark': 'LongType'},
        'float' : {'glue' : 'float', 'spark': 'FloatType'},
        'date' : {'glue' : 'date', 'spark': 'DateType'},
        'datetime' : {'glue' : 'double', 'spark': 'DoubleType'},
        'boolean' : {'glue' : 'boolean', 'spark': 'BooleanType'}
    }

    def __init__(self, filepath) :
        meta = read_json(filepath)
        self.columns = meta['columns']
        self.name = meta['table_name']
        self.description = meta['table_desc']
        self.data_format = meta['data_format']
        self.id = meta['id']
        self.location = meta['location']
        if 'partitions' in meta :
            self.partitions = meta['partitions']
        else :
            self.partitions = []
    
    # # # Getter and setter functions
    # Columns
    @property
    def columns(self) :
        return self._columns

    @columns.setter
    def columns(self, columns) :
        self._columns = columns

    # column_names
    @property
    def column_names(self) :
        return [c['name'] for c in self._columns]

    # table name
    @property
    def name(self) :
        return self._name

    @name.setter
    def name(self, name) :
        self._name = name

    # table description
    @property
    def table_description(self) :
        return self._table_description

    @table_description.setter
    def table_description(self, table_description) :
        self._table_description = table_description

    # data format
    @property
    def data_format(self) :
        return self._data_format

    @data_format.setter
    def data_format(self, data_format) :
        self._data_format = data_format

    # id
    @property
    def id(self) :
        return self._id

    @id.setter
    def id(self, id) :
        self._id = id

    # partitions
    @property
    def partitions(self) :
        return self._partitions

    @partitions.setter
    def partitions(self, partitions) :
        if partitions is None :
            partitions = []
        else :
            for p in partitions : self._check_column_exists(p)
            self._partitions = partitions

    @property
    def location(self) :
        return self._location

    @location.setter
    def location(self, location) :
        _validate_string(location, allowed_chars="_/")
        if location[0] == '/' or location[-1] != '/':
            raise ValueError("location should not start with a slash and end with a slash")
        self._location = location
        
    def generate_glue_columns(self, exclude_columns = []) :

        glue_columns = []
        for c in self.columns :
            if c['name'] not in exclude_columns :
                new_c = {}
                new_c["Name"] = c["name"]
                new_c["Comment"] = c["description"]
                new_c["Type"] = self._agnostic_to_glue_spark_dict[c['type']]['glue']
                glue_columns.append(new_c)
    
        return glue_columns

    def _check_valid_datatype(self, data_type) :
        if data_type not in self._supported_column_types :
            raise ValueError("The data_type provided must match the supported data_type names: {}".format(", ".join(self._supported_column_types)))

    def _check_column_exists(self, column_name) :
        if column_name not in self.column_names :
            raise ValueError("The column name does not match those existing in meta: {}".format(", ".join(self.column_names)))
        
    def update_column(self, column_name, new_name = None, new_data_type = None, new_description = None) :
        
        self._check_column_exists(column_name)

        if new_name is None and new_data_type is None and new_description is None :
            raise ValueError("one or more of the function inputs (new_name, new_data_type and new_description) must be specified.")
        new_cols = []
        for c in self.columns :
            if c['name'] == column_name :
                _validate_string(new_name, "_")

                if new_name is not None :
                    c['name'] = new_name
                
                if new_data_type is not None :
                    self._check_valid_datatype(new_data_type)
                    c['type'] = new_data_type

                if new_description is not None :
                    _validate_string(new_description, "_,.")
                    c['description'] = new_description

            new_cols.append(c)
        
        self.columns = new_cols

    def glue_table_definition(self, full_database_path) :
        # base_io = _conversion['base']
        glue_table_definition = _get_spec('base')
        # specific_io = _conversion[self.data_format]
        # specific = json.load(specific_io)
        specific = _get_spec(self.data_format)

        dict_merge(glue_table_definition, specific)
        
        # Create glue specific variables from meta data
        glue_table_definition["Name"] = self.name
        glue_table_definition["Description"] = self.description

        glue_table_definition['StorageDescriptor']['Columns'] = self.generate_glue_columns(exclude_columns = self.partitions)
        glue_table_definition['StorageDescriptor']["Location"] = full_database_path + self.location

        if len(self.partitions) > 0 :
            not_partitions = [c for c in self.column_names if c not in self.partitions]
            glue_partition_cols = self.generate_glue_columns(exclude_columns = not_partitions)

            glue_table_definition['PartitionKeys'] = glue_partition_cols

        return glue_table_definition

    def write_to_json(self, file_path) :
        write_obj = {
            "id" : self.id,
            "table_name" : self.name,
            "table_description" : self.table_description,
            "data_format" : self.data_format,
            "columns" : self.column_names,
            "partitions" : self.partitions
        }
        write_json(write_obj, file_path)


class Database_Meta :

    def __init__(self, database_folder_path, db_suffix = '_dev') :
        self._tables = []
        if database_folder_path[-1] != '/' :
            database_folder_path = database_folder_path + '/'
        
        db_meta = read_json(database_folder_path + 'database.json')

        self.name = db_meta['name']
        self.bucket = db_meta['bucket']
        self.base_folder = db_meta['bucket']
        self.location = db_meta['location']
        self.description = db_meta['description']
        self.db_suffix = db_suffix
        files = os.listdir(database_folder_path)
        files = set([f for f in files if re.match(".+\.json$", f)])

        for f in files :
            if 'database.json' not in f :
                self.add_table(Table_Meta(database_folder_path + f))
        
    @property
    def name(self):
        return self._name

    @name.setter 
    def name(self, name) :
        self._name = name
    
    @property
    def bucket(self):
        return self._bucket

    @bucket.setter 
    def bucket(self, bucket) :
        self._bucket = bucket
    
    @property
    def base_folder(self):
        return self._base_folder

    @base_folder.setter 
    def base_folder(self, base_folder) :
        self._base_folder = base_folder
    
    @property
    def location(self):
        return self._location

    @location.setter 
    def location(self, location) :
        self._location = location
    
    @property
    def description(self):
        return self._description

    @description.setter 
    def description(self, description) :
        self._description = description

    @property
    def table_names(self) :
        table_names = [t.name for t in self._tables]
        return table_names
    
    @property
    def db_suffix(self) :
        return self._db_suffix

    @db_suffix.setter
    def db_suffix(self, db_suffix) :
        _validate_string(db_suffix)
        self._db_suffix = db_suffix
        return self._db_suffix

    @property
    def glue_name(self) :
        return self.name + self.db_suffix
    
    @property
    def s3_base_folder(self) :
        return self.base_folder + self.db_suffix

    @property
    def s3_database_path(self) :
        return "s3://{}/{}/{}/".format(self.bucket, self.s3_base_folder, self.location)
 
    def _check_table_exists(self, table_name) : 
        return table_name in self.table_names

    def _throw_error_check_table(self, table_name, error_on_table_exists = True) :
        
        error_string = "Table {} already exists.".format(table_name) if error_on_table_exists else "Table {} does not exist.".format(table_name)
        if self._check_table_exists(table_name) == error_on_table_exists:
            raise ValueError(error_string)

    def table(self, table_name) :
        self._throw_error_check_table(table_name, error_on_table_exists = False)
        out = [t for t in self._tables if t.name == table_name][0]
        return out

    def add_table(self, table) :
        self._throw_error_check_table(table.name)
        self._tables.append(table)

    def remove_table(self, table_name) :
        self._throw_error_check_table(table_name, False)
        self._tables = [t for t in self._tables if t.name != table_name]

    def create_glue_database(self) :
        """
        Creates a database in Glue.  If it exists, delete it
        """
        db = {
            "DatabaseInput": { 
                "Description": self.description,
                "Name": self.glue_name,
            }
        }

        try:
            _glue_client.delete_database(Name = self.glue_name)
        except :
            pass
        _glue_client.create_database(**db)

        for tab in self._tables :
            glue_table_def = tab.glue_table_definition(self.s3_database_path)
            _glue_client.create_table(DatabaseName = self.glue_name, TableInput = glue_table_def)