import collections
import json
import boto3
import tempfile
import zipfile
import shutil
import string
import os
import subprocess

_glue_client = boto3.client('glue', 'eu-west-1')
_athena_client = boto3.client('athena', 'eu-west-1')
_s3_client = boto3.client('s3')
_s3_resource = boto3.resource('s3')

def _get_git_revision_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').replace('\n','')

def _get_git_revision_short_hash():
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('utf-8').replace('\n','')


# https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def _dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            _dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]

# Read json file
def read_json(filename) :
    with open(filename) as json_data:
        data = json.load(json_data)
    return data

# Write json file
def write_json(data, filename) :
    with open(filename, 'w+') as outfile:
        json.dump(data, outfile, indent=4, separators=(',', ': '))

def _end_with_slash(string) :
    if string[-1] != '/' :
        return string + '/'
    else :
        return string

def _remove_final_slash(string) :
    if string[-1] == '/' :
        return string[:-1]
    else:
        return string

# Used by both classes (Should move into another module)
def _validate_string(s, allowed_chars = "_") :
    if s != s.lower() :
        raise ValueError("string provided must be lowercase")

    invalid_chars = string.punctuation

    for a in allowed_chars :
        invalid_chars = invalid_chars.replace(a, "")

    if any(char in invalid_chars for char in s) :
        raise ValueError("punctuation excluding ({}) is not allowed in string".format(allowed_chars))

def _get_file_from_file_path(file_path) :
    return file_path.split('/')[-1]

def _unnest_github_zipfile_and_return_new_zip_path(zip_path):
    """
    When we download a zipball from github like this one:
    https://github.com/moj-analytical-services/gluejobutils/archive/master.zip

    The python lib is nested in the directory like:
    gluejobutils-master/gluejobutils/py files

    The glue docs say that it will only work without this nesting:
    docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html

    This function creates a new, unnested zip file, and returns the path to it

    """

    original_file_name = os.path.basename(zip_path)
    original_dir = os.path.dirname(zip_path)
    new_file_name = original_file_name.replace(".zip", "_new")

    with tempfile.TemporaryDirectory() as td:
        myzip = zipfile.ZipFile(zip_path, 'r')
        myzip.extractall(td)
        nested_folder_to_unnest = os.listdir(td)[0]
        nested_path = os.path.join(td, nested_folder_to_unnest)
        output_path = os.path.join(original_dir, new_file_name)
        final_output_path = shutil.make_archive(output_path, 'zip', nested_path)

    return final_output_path
