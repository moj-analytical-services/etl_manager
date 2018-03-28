from mojdbtemplate.utils import _read_json, _write_json, _dict_merge, _end_with_slash, _validate_string, _glue_client
from copy import copy
import string
import json
import os
import re
import pkg_resources

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

def _get_spec(spec_name) :
    if spec_name not in _template :
        raise ValueError("spec_name/data_type requested ({}) is not a valid spec/data_type".format(spec_name))

    return _template[spec_name]

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
        meta = _read_json(filepath)
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
        
        glue_table_definition = _get_spec('base')
        specific = _get_spec(self.data_format)
        _dict_merge(glue_table_definition, specific)
        
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
        _write_json(write_obj, file_path)


class Database_Meta :
    """
    Python class to manage glue databases from our agnostic meta data. 
    db = Database_Meta('path_to_local_meta_data_folder/')
    This will create a database object that also holds table objects for each table json in the folder it is pointed to.
    The meta data folder used to initialise the database must contain a database.json file.
    """
    def __init__(self, database_folder_path, db_suffix = '_dev') :
        
        self._tables = []
        database_folder_path = _end_with_slash(database_folder_path)
        
        db_meta = _read_json(database_folder_path + 'database.json')

        self.name = db_meta['name']
        self.bucket = db_meta['bucket']
        self.base_folder = db_meta['base_folder']
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
        """
        Name of the database. When used to create the database in s3 this is suffixed with db.sb_suffix.
        """
        return self._name

    @name.setter 
    def name(self, name) :
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
    def base_folder(self):
        """
        The base folder is the path to the database build. This path is relative to the root dir of the bucket. It is also suffixed by db_suffix when creating the glue database.
        e.g. if db.base_folder = 'v1' denoting the first version of the database build. Then the path to the s3 base folder for this database would be 'v1_dev' providing that db.db_suffix == '_dev'.
        """
        return self._base_folder

    @base_folder.setter 
    def base_folder(self, base_folder) :
        base_folder = _end_with_slash(base_folder)
        self._base_folder = base_folder
    
    @property
    def location(self):
        """
        Location of the database. This folder path is relative to the base_folder path. E.g. the database might be in my_database/
        """
        return self._location

    @location.setter 
    def location(self, location) :
        location = _end_with_slash(location)
        self._location = location
    
    @property
    def description(self):
        """
        The database's description.
        """
        return self._description

    @description.setter 
    def description(self, description) :
        self._description = description

    @property
    def table_names(self) :
        """
        Returns the names of the table objects in the database object.
        """
        table_names = [t.name for t in self._tables]
        return table_names
    
    @property
    def db_suffix(self) :
        """
        The suffix that is added to the database. Default is _dev. The suffix is used to create different build of the database in glue.
        """
        return self._db_suffix

    @db_suffix.setter
    def db_suffix(self, db_suffix) :
        if db_suffix is not None or db_suffix != '' :
            _validate_string(db_suffix, "_-")
            self._db_suffix = db_suffix
        else :
            self._db_suffix = db_suffix
        return self._db_suffix

    @property
    def glue_name(self) :
        """
        Returns the name of the database in the aws glue catalogue.
        """
        return self.name + self.db_suffix
    
    @property
    def s3_base_folder(self) :
        """
        Returns what the base_folder will be in S3. This is the database object's base_folder plus and db_suffix. 
        """
        return self.base_folder[:-1] + self.db_suffix + '/'

    @property
    def s3_database_path(self) :
        """
        Returns the s3 path to the database
        """
        return "s3://{}/{}{}".format(self.bucket, self.s3_base_folder, self.location)
 
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
        table must be a table object e.g. table = Table_Meta(example_meta_data/employees.json)
        """
        if not isinstance(table, Table_Meta) :
            raise ValueError("table must an object of Table_Meta class")
        self._throw_error_check_table(table.name)
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
        Deletes a glue database with the same name (db.alias_name). Returns a response explaining if it was deleted or didn't delete because database was not found.
        """
        try :
            _glue_client.delete_database(Name = self.glue_name)
            response = 'database deleted'
        except :
            response = 'Cannot delete as database not found in glue catalogue'
        return response

    def create_glue_database(self) :
        """
        Creates a database in Glue based on the database object calling the method function. If a database with the same name (db.glue_name) already exists it overwrites it.
        """
        db = {
            "DatabaseInput": { 
                "Description": self.description,
                "Name": self.glue_name,
            }
        }

        del_resp = self.delete_glue_database()

        _glue_client.create_database(**db)

        for tab in self._tables :
            glue_table_def = tab.glue_table_definition(self.s3_database_path)
            _glue_client.create_table(DatabaseName = self.glue_name, TableInput = glue_table_def)

    def write_to_json(self, folder_path, write_tables = True) :
        """
        Writes the database object back into the agnostic meta data json files.
        Function writes a file called database.json to the folder_path provided.
        If write_tables is True (default) this method will also write all table objects as an agnostic meta data json. 
        The table meta data json will be saved as <table_name>.json where table_name == table.name.
        """
        folder_path = _end_with_slash(folder_path)
        db_write_obj = {
            "description": self.description,
            "name": self.name,
            "bucket": self.bucket,
            "base_folder": self.base_folder,
            "location": self.location
        }
        _write_json(db_write_obj, folder_path + 'database.json')

        if write_tables : 
            for t in self._tables :
                t._write_json(folder_path + t.name + '.json')
