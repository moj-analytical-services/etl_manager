import copy
import json
import boto3
import tempfile
import zipfile
import shutil
import string
import os
import subprocess

from collections.abc import Mapping

import regex

_glue_client = boto3.client("glue", "eu-west-1")
_athena_client = boto3.client("athena", "eu-west-1")
_s3_client = boto3.client("s3")
_s3_resource = boto3.resource("s3")


def _get_git_revision_hash():
    return (
        subprocess.check_output(["git", "rev-parse", "HEAD"])
        .decode("utf-8")
        .replace("\n", "")
    )


def _get_git_revision_short_hash():
    return (
        subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
        .decode("utf-8")
        .replace("\n", "")
    )


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
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], Mapping):
            _dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]


# Read json file
def read_json(filename):
    with open(filename) as json_data:
        data = json.load(json_data)
    return data


# Write json file
def write_json(data, filename):
    with open(filename, "w+") as outfile:
        json.dump(data, outfile, indent=4, separators=(",", ": "))


def _end_with_slash(string):
    if string[-1] != "/":
        return string + "/"
    else:
        return string


def _remove_final_slash(string):
    if string[-1] == "/":
        return string[:-1]
    else:
        return string


# Used by both classes (Should move into another module)
def _validate_string(s, allowed_chars="_", allow_upper=False):
    if s != s.lower() and not allow_upper:
        raise ValueError("string provided must be lowercase")

    invalid_chars = string.punctuation

    for a in allowed_chars:
        invalid_chars = invalid_chars.replace(a, "")

    if any(char in invalid_chars for char in s):
        raise ValueError(
            f"punctuation excluding ({allowed_chars}) is not allowed in string"
        )


def _validate_enum(enum):
    if type(enum) != list:
        raise TypeError(f"enum must be a list. Not of type {type(enum)}")


def _validate_pattern(pattern):
    if type(pattern) != str:
        raise TypeError(f"pattern must be a string. Not of type {type(pattern)}")


def _validate_nullable(nullable):
    if type(nullable) != bool:
        raise TypeError(f"nullable must be a boolean. Not of type {type(nullable)}")


def _validate_sensitivity(sensitivity):
    if not isinstance(sensitivity, str):
        raise TypeError(
            f"sensitivity must be a string. Not of type {type(sensitivity)}"
        )


def _validate_redacted(redacted):
    if not isinstance(redacted, bool):
        raise TypeError(f"redacted must be a boolean. Not of type {type(redacted)}")


def _get_file_from_file_path(file_path):
    return file_path.split("/")[-1]


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
        myzip = zipfile.ZipFile(zip_path, "r")
        myzip.extractall(td)
        nested_folder_to_unnest = os.listdir(td)[0]
        nested_path = os.path.join(td, nested_folder_to_unnest)
        output_path = os.path.join(original_dir, new_file_name)
        final_output_path = shutil.make_archive(output_path, "zip", nested_path)

    return final_output_path


# Note the ?R recursive.  We only allow 'character' (the agnostic type), in a non-complex type, but must allow string within complex types.
# User will still get an error for string as non-complex type from the schema.
COL_TYPE_REGEX = regex.compile(
    r"(character|int|long|float|double|date|datetime|boolean|binary|decimal\(\d+,\d+\)|struct<(([a-zA-Z_]+):((?R)(,?)))+>|array<(?R)>)"
)


def data_type_is_regex(data_type):
    return bool(regex.match(COL_TYPE_REGEX, data_type))


def glue_type_to_dict(data_type):
    type_dict = {}
    splits = data_type.split("<", 1)
    name = splits[0].rstrip(">")
    if len(splits) > 1:
        defs = splits[1].rstrip(">")
        if name == "struct":
            ntd = {}
            for data_type in defs.split(","):
                if ":" in data_type:
                    nn, dt = data_type.split(":")
                    ntd[nn] = glue_type_to_dict(dt)
            type_dict[name] = ntd
        else:
            type_dict[name] = glue_type_to_dict(defs)
        return type_dict
    return name


def trim_complex_type(col_type):
    return col_type.split("(", 1)[0].split("<", 1)[0]


def trim_complex_data_types(json_data):
    updated = copy.deepcopy(json_data)
    for index, col in enumerate(json_data["columns"]):
        updated["columns"][index]["type"] = trim_complex_type(col["type"])
    return updated


def s3_path_to_bucket_key(s3_path):
    """
    Splits out s3 file path to bucket key combination
    """
    return s3_path.replace("s3://", "").split("/", 1)
