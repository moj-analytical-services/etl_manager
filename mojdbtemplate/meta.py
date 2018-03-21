from dataengineeringutils.utils import read_json, write_json, dict_merge
from copy import copy
import string
import json
import os
import re

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

    # def _check_valid_datatype(self, func) :
    #     def inner(*args, **kwargs) :
    #         if kwargs('data_type') not in self._supported_column_types :
    #             raise ValueError("The data_type provided must match the supported data_type names: {}".format(", ".join(self._supported_column_types)))
    #         else :
    #             func(*args, **kwargs)
    #     return inner

    # def _check_column_exists(self, func) :
    #     def inner(*args, **kwargs) :
    #         if column_name not in self.column_names :
    #             raise ValueError("The column name does not match those existing in meta: {}".format(", ".join(self.column_names)))

    def _check_valid_datatype(self, data_type) :
        if data_type not in self._supported_column_types :
            raise ValueError("The data_type provided must match the supported data_type names: {}".format(", ".join(self._supported_column_types)))

    def _check_column_exists(self, column_name) :
        if column_name not in self.column_names :
            raise ValueError("The column name does not match those existing in meta: {}".format(", ".join(self.column_names)))
        
    def _validate_string(self, s, allowed_chars = "_") :
        if s != s.lower() :
            raise ValueError("string provided must be lowercase")
        
        invalid_chars = string.punctuation

        for a in allowed_chars :
            invalid_chars = invalid_chars.replace(a, "")
        
        if any(char in invalid_chars for char in s) :
            raise ValueError("punctuation excluding ({}) is not allowed in string".format(allowed_chars))
        

    def update_column(self, column_name, new_name = None, new_data_type = None, new_description = None) :
        
        self._check_column_exists(column_name)

        if new_name is None and new_data_type is None and new_description is None :
            raise ValueError("one or more of the function inputs (new_name, new_data_type and new_description) must be specified.")
        new_cols = []
        for c in self.columns :
            if c['name'] == column_name :
                self._validate_string(new_name, "_")

                if new_name is not None :
                    c['name'] = new_name
                
                if new_data_type is not None :
                    self._check_valid_datatype(new_data_type)
                    c['type'] = new_data_type

                if new_description is not None :
                    self._validate_string(new_description, "_,.")
                    c['description'] = new_description

            new_cols.append(c)
        
        self.columns = new_cols

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

    def __init__(self, database_folder_path) :
        self._tables = []
        if database_folder_path[-1] != '/' :
            database_folder_path = database_folder_path + '/'
        
        db_meta = read_json(database_folder_path + 'database.json')

        self.name = db_meta['name']
        self.location = db_meta['location']
        self.description = db_meta['description']

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
    
    def add_table(self, table) :
        self._check_table_exists(table.name)
        self._tables.append(table)

    @property
    def table_names(self) :
        table_names = [t.name for t in self._tables]
        return(table_names)
    
    def _check_table_exists(self, table_name) : 
        if table_name in [self.table_names] :
            raise ValueError("database meta object already has a table with name: {}. You might have duplicate json objects in your database folder.".format(table_name))
    
    def table(self, table_name) :
        self._check_table_exists(table_name)
        out = [t for t in self._tables if t.name == table_name][0]
        return(out)

    def delete_table(self, table_name) :
        self._check_table_exists(table_name)
        i = 0 
        for tab in self._tables :
            if tab.name == table_name :
                break
            i += 1
        del self._tables[i]
