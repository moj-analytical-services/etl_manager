import datetime
import glob
import os
import re
import shutil
import tempfile
import time
from typing import Optional, Union
import zipfile
from urllib.request import urlretrieve

from botocore.exceptions import ClientError

from etl_manager.utils import _glue_client, _s3_client, _s3_resource, _validate_string

# Create temp folder - upload to s3
# Lock it in
# Run job - check if job exists and lock in temp folder matches


class JobMisconfigured(Exception):
    pass


class JobNotStarted(Exception):
    pass


class JobFailed(Exception):
    pass


class JobTimedOut(Exception):
    pass


class JobStopped(Exception):
    pass


class JobThrottlingExceeded(Exception):
    pass


class GlueJob:
    """
    Take a folder structure on local disk.

    Folder must be formatted as follows:
    job_folder
      job.py
      glue_py_resources/
        zip and python files
        github_zip_urls.txt <- file containing urls of zip files from github, which will be converted into glue's required format
      glue_resources/
        txt, sql, json, or csv files
      glue_jars/
        jar files

    Can then run jobs on aws glue using this class.

    If include_shared_job_resources is True then glue_py_resources and glue_resources folders inside a special named folder 'shared_glue_resources'
    will also be referenced.
    glue_jobs (parent folder to 'job_folder')
      shared_glue_resources
        glue_py_resources/
          zip, python and zip_urls
        glue_resources/
          txt, sql, json, or csv files
        glue_jars/
            jar files
      job_folder
        etc...
    """

    def __init__(
        self,
        job_folder,
        bucket,
        job_role,
        job_name=None,
        job_arguments={},
        include_shared_job_resources=True,
        timeout_override_minutes=None,
        tags: Optional[dict] = None,
    ):
        self.GLUE_WORKER_HOURLY_COST = 0.44  # i.e. 44 cents per worker per hour
        self.MAXIMUM_COST_TIMEOUT = (
            20  # i.e by default, jobs will timeout if they cost > 20 dollars
        )
        self.job_id = "{:0.0f}".format(time.time())

        job_folder = os.path.normpath(job_folder)
        self._job_folder = job_folder

        if not os.path.exists(self.job_path):
            raise ValueError(
                (
                    f"Could not find job.py in base directory provided "
                    f"({job_folder}), stopping.\nOnly folder allowed to "
                    f"have no job.py is a folder named shared_job_resources"
                )
            )

        self.bucket = bucket
        if job_name is None:
            self.job_name = os.path.basename(self._job_folder)
        else:
            self.job_name = job_name

        self.job_role = job_role
        self.include_shared_job_resources = include_shared_job_resources
        self.py_resources = self._get_py_resources()
        self.resources = self._get_resources()
        self.jars = self._get_jars()
        self.all_meta_data_paths = (
            self._get_metadata_paths()
        )  # Within a glue job, it's sometimes useful to be able to access the agnostic metdata
        self.github_zip_urls = self._get_github_resource_list()

        self.job_arguments = job_arguments

        self.timeout_override_minutes = timeout_override_minutes
        self.tags = tags if tags is not None else dict()

        self.github_py_resources = []

        # Set as default can change using standard getters and setters (except _job_run_id only has getter)
        self._job_run_id = None
        self.max_retries = 0
        self.max_concurrent_runs = 1
        self.number_of_workers = 2
        self.worker_type = "G.1X"
        self.glue_version = "2.0"
        self.python_version = "3"
        self.pip_requirements = None

    @property
    def timeout(self):
        if self.timeout_override_minutes is None:
            # "G.2X" -> 2.0
            dpu_per_worker = float(self.worker_type[2]+'.'+self.worker_type[3:-1])
            # 60 because timeout is in munites, whereas glue worker cost is in hours
            return int(
                60
                * (
                    self.MAXIMUM_COST_TIMEOUT
                    / (self.GLUE_WORKER_HOURLY_COST * dpu_per_worker * self.number_of_workers)
                )
            )
        else:
            return int(self.timeout_override_minutes)

    @property
    def job_folder(self):
        return self._job_folder

    @property
    def job_path(self):
        return os.path.join(self.job_folder, "job.py")

    @property
    def s3_job_folder_inc_bucket(self):
        return f"s3://{self.bucket}/{self.s3_job_folder_no_bucket}"

    @property
    def s3_job_folder_no_bucket(self):
        return os.path.join("_GlueJobs_", self.job_name, self.job_id, "resources/")

    @property
    def s3_metadata_base_folder_inc_bucket(self):
        return os.path.join(self.s3_job_folder_inc_bucket, "meta_data")

    @property
    def s3_metadata_base_folder_no_bucket(self):
        return os.path.join(self.s3_job_folder_no_bucket, "meta_data")

    @property
    def job_parent_folder(self):
        return os.path.dirname(self.job_folder)

    @property
    def etl_root_folder(self):
        return os.path.dirname(self.job_parent_folder)

    @property
    def job_arguments(self):
        metadata_argument = {
            "--metadata_base_path": self.s3_metadata_base_folder_inc_bucket
        }
        return {**self._job_arguments, **metadata_argument}

    @job_arguments.setter
    def job_arguments(self, job_arguments):
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        if job_arguments is not None:
            if not isinstance(job_arguments, dict):
                raise ValueError("job_arguments must be a dictionary")
            # validate dict keys
            special_aws_params = [
                "--JOB_NAME",
                "--debug",
                "--mode",
                "--metadata_base_path",
                "--additional-python-modules"
            ]
            for k in job_arguments.keys():
                if k[:2] != "--" or k in special_aws_params:
                    awsp = ", ".join(special_aws_params)
                    raise ValueError(
                        (
                            f"Found incorrect AWS job argument ({k}). All "
                            f"arguments should begin with '--' and cannot be "
                            f"one of the following: {awsp}"
                        )
                    )
        self._job_arguments = job_arguments

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, bucket):
        _validate_string(bucket, "-,")
        self._bucket = bucket

    @property
    def job_name(self):
        return self._job_name

    @job_name.setter
    def job_name(self, job_name):
        _validate_string(job_name, allowed_chars="-_:")
        self._job_name = job_name

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags: dict):
        if not isinstance(tags, dict):
            raise TypeError("tags must be of type dict")
        self._tags = tags

    @property
    def job_run_id(self):
        return self._job_run_id

    @property
    def glue_version(self):
        return self._glue_version

    @glue_version.setter
    def glue_version(self, v):
        valid_glue_versions = ["4.0", "3.0", "2.0", "1.0", "0.9"]
        if not isinstance(v, str):
            raise TypeError(f"glue_version must be of type str (given {type(v)})")
        if v not in valid_glue_versions:
            raise ValueError(
                f"glue_version must be one of {valid_glue_versions} (give {v})"
            )
        self._glue_version = v

    @property
    def worker_type(self):
        return self._worker_type

    @worker_type.setter
    def worker_type(self, t):
        valid_worker_types = ["G.1X", "G.2X", "G.4X", "G.8X", "G.025X"]
        if not isinstance(t, str):
            raise TypeError(f"worker_type must be of type str (given {type(t)})")
        if t not in valid_worker_types:
            raise ValueError(
                f"worker_type must be one of {valid_worker_types} (give {t})"
            )
        self._worker_type = t

    @property
    def python_version(self):
        return self._python_version

    @python_version.setter
    def python_version(self, v):
        valid_python_versions = ["3", "2"]
        if not isinstance(v, str):
            raise TypeError(f"python_version must be of type str (given {type(v)})")
        if v not in valid_python_versions:
            raise ValueError(
                f"python_version must be one of {valid_python_versions} (give {v})"
            )

        self._python_version = v

    @property
    def pip_requirements(self):
        return self._pip_requirements

    @pip_requirements.setter
    def pip_requirements(self, requirements: Union[str, None]):
        glue_version = self.glue_version
        if glue_version not in ["4.0", "3.0", "2.0"]:
            raise ValueError(
                f"pip_requirements cannot be set for Glue {glue_version}"
            )
        if requirements is not None:
            if not isinstance(requirements, str):
                raise TypeError(
                    f"pip_requirements must be a str (given {type(requirements)}"
                )
            try:
                _, ext = os.path.splitext(requirements)
            except Exception:
                raise ValueError("requirements should be a valid filepath")
            if ext != ".txt":
                raise ValueError("requirements should be a filepath to a txt file")

        self._pip_requirements = requirements

    def _get_pip_requirements(self):
        req_txt = self.pip_requirements
        if req_txt is not None:
            with open(req_txt, 'r') as f:
                req = f.readlines()

            req_str = ",".join(req).replace("\n", '')

            return req_str

    def _check_nondup_resources(self, resources_list):
        file_list = [os.path.basename(r) for r in resources_list]
        if len(file_list) != len(set(file_list)):
            raise ValueError(
                (
                    "There are duplicate file names in your supplied "
                    "resources. A file in job resources might share the "
                    "same name as a file in the shared resources folders."
                )
            )

    def _get_github_resource_list(self):
        zip_urls_path = os.path.join(
            self.job_folder, "glue_py_resources", "github_zip_urls.txt"
        )
        shared_zip_urls_path = os.path.join(
            self.job_parent_folder,
            "shared_job_resources",
            "glue_py_resources",
            "github_zip_urls.txt",
        )

        if os.path.exists(zip_urls_path):
            with open(zip_urls_path, "r") as f:
                urls = f.readlines()
            f.close()
        else:
            urls = []

        if os.path.exists(shared_zip_urls_path) and self.include_shared_job_resources:
            with open(shared_zip_urls_path, "r") as f:
                shared_urls = f.readlines()
            f.close()
            urls = urls + shared_urls

        urls = [url for url in urls if len(url) > 10]

        return urls

    def _list_folder_with_regex(self, folder_path, regex):
        if os.path.isdir(folder_path):
            listing = os.listdir(folder_path)
            listing_filtered = [f for f in listing if re.match(regex, f)]
            listing_filtered = [os.path.join(folder_path, f) for f in listing_filtered]
            return listing_filtered
        else:
            return []

    def _get_py_resources(self):
        # Upload all the .py or .zip files in resources
        # Check existence of folder, otherwise skip

        resource_folder = "glue_py_resources"
        regex = ".+(\.py|\.zip)$"

        resources_path = os.path.join(self.job_folder, resource_folder)
        shared_resources_path = os.path.join(
            self.job_parent_folder, "shared_job_resources", resource_folder
        )

        resource_listing = self._list_folder_with_regex(resources_path, regex)

        if self.include_shared_job_resources:
            shared_resource_listing = self._list_folder_with_regex(
                shared_resources_path, regex
            )
            resource_listing = resource_listing + shared_resource_listing

        return resource_listing

    def _get_resources(self):
        # Upload all the .py or .zip files in resources
        # Check existence of folder, otherwise skip

        resource_folder = "glue_resources"
        regex = ".+(\.sql|\.json|\.csv|\.txt)$"

        resources_path = os.path.join(self.job_folder, resource_folder)
        shared_resources_path = os.path.join(
            self.job_parent_folder, "shared_job_resources", resource_folder
        )

        resource_listing = self._list_folder_with_regex(resources_path, regex)

        if self.include_shared_job_resources:
            shared_resource_listing = self._list_folder_with_regex(
                shared_resources_path, regex
            )
            resource_listing = resource_listing + shared_resource_listing

        return resource_listing

    def _get_jars(self):
        # Upload all the .jar files in the jars folder
        # Check existence of folder, otherwise skip

        resource_folder = "glue_jars"
        regex = ".+(\.jar)$"

        resources_path = os.path.join(self.job_folder, resource_folder)
        shared_resources_path = os.path.join(
            self.job_parent_folder, "shared_job_resources", resource_folder
        )

        resource_listing = self._list_folder_with_regex(resources_path, regex)

        if self.include_shared_job_resources:
            shared_resource_listing = self._list_folder_with_regex(
                shared_resources_path, regex
            )
            resource_listing = resource_listing + shared_resource_listing

        return resource_listing

    def _get_metadata_paths(self):
        """
        Enumerate the relative path for all metadata json files
        """

        metadata_base = os.path.join(self.etl_root_folder, "meta_data")
        all_files = list(glob.iglob(metadata_base + "/**/*.json", recursive=True))

        # Remove everything up to the main meta_data path
        return list(all_files)

    def _download_github_zipfile_and_rezip_to_glue_file_structure(self, url):
        this_zip_path = os.path.join(
            f"_{self.job_name}_tmp_zip_files_to_s3_", "github.zip"
        )
        urlretrieve(url, this_zip_path)

        original_dir = os.path.dirname(this_zip_path)

        with tempfile.TemporaryDirectory() as td:
            myzip = zipfile.ZipFile(this_zip_path, "r")
            myzip.extractall(td)
            nested_folder_to_unnest = os.listdir(td)[0]
            nested_path = os.path.join(td, nested_folder_to_unnest)
            name = url.split("/")[4]
            output_path = os.path.join(original_dir, name)
            final_output_path = shutil.make_archive(output_path, "zip", nested_path)

        os.remove(this_zip_path)

        return final_output_path

    def sync_job_to_s3_folder(self):
        # Test if folder exists and create if not
        temp_folder_already_exists = False
        temp_zip_folder = f"_{self.job_name}_tmp_zip_files_to_s3_"
        if os.path.exists(temp_zip_folder):
            temp_folder_already_exists = True
        else:
            os.makedirs(temp_zip_folder)

        # Download the github urls and rezip them to work with aws glue
        self.github_py_resources = []
        for url in self.github_zip_urls:
            self.github_py_resources.append(
                self._download_github_zipfile_and_rezip_to_glue_file_structure(url)
            )

        # Check if all filenames are unique
        files_to_sync = (
            self.github_py_resources
            + self.py_resources
            + self.resources
            + self.jars
            + [self.job_path]
        )
        self._check_nondup_resources(files_to_sync)

        # delete the tmp folder before uploading new data to it
        self.delete_s3_job_temp_folder()

        # Sync all job resources to the same s3 folder
        for f in files_to_sync:
            s3_file_path = os.path.join(
                self.s3_job_folder_no_bucket, os.path.basename(f)
            )
            _s3_client.upload_file(f, self.bucket, s3_file_path)

        # Upload metadata to subfolder
        for f in self.all_meta_data_paths:
            path_within_metadata_folder = re.sub("^.*/?meta_data/", "", f)
            s3_file_path = os.path.join(
                self.s3_metadata_base_folder_no_bucket, path_within_metadata_folder
            )
            _s3_client.upload_file(f, self.bucket, s3_file_path)

        #  Clean up downloaded zip files
        for f in list(self.github_py_resources):
            os.remove(f)
        if not temp_folder_already_exists:
            os.rmdir(temp_zip_folder)

    def _job_definition(self):
        script_location = os.path.join(self.s3_job_folder_inc_bucket, "job.py")
        tmp_dir = os.path.join(self.s3_job_folder_inc_bucket, "glue_temp_folder/")

        job_definition = {
            "Name": self.job_name,
            "Role": self.job_role,
            "ExecutionProperty": {"MaxConcurrentRuns": self.max_concurrent_runs},
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": self.python_version,
            },
            "DefaultArguments": {
                "--TempDir": tmp_dir,
                "--extra-files": "",
                "--extra-py-files": "",
                "--job-bookmark-option": "job-bookmark-disable",
            },
            "MaxRetries": self.max_retries,
            "WorkerType": self.worker_type,
            "NumberOfWorkers": self.number_of_workers,
            "GlueVersion": self.glue_version,
            "Tags": self.tags,
            "Timeout": self.timeout,
        }

        if self.pip_requirements is not None:
            pip_req = self._get_pip_requirements()
            job_definition["DefaultArguments"]["--additional-python-modules"] = pip_req

        if len(self.resources) > 0:
            extra_files = ",".join(
                [
                    os.path.join(self.s3_job_folder_inc_bucket, os.path.basename(f))
                    for f in self.resources
                ]
            )
            job_definition["DefaultArguments"]["--extra-files"] = extra_files
        else:
            job_definition["DefaultArguments"].pop("--extra-files", None)

        if len(self.py_resources) > 0 or len(self.github_py_resources) > 0:
            extra_py_files = ",".join(
                [
                    os.path.join(self.s3_job_folder_inc_bucket, os.path.basename(f))
                    for f in (self.py_resources + self.github_py_resources)
                ]
            )
            job_definition["DefaultArguments"]["--extra-py-files"] = extra_py_files
        else:
            job_definition["DefaultArguments"].pop("--extra-py-files", None)

        if len(self.jars) > 0:
            extra_jars = ",".join(
                [
                    os.path.join(self.s3_job_folder_inc_bucket, os.path.basename(f))
                    for f in (self.jars)
                ]
            )
            job_definition["DefaultArguments"]["--extra-jars"] = extra_jars
        else:
            job_definition["DefaultArguments"].pop("--extra-jars", None)

        return job_definition

    def run_job(self, sync_to_s3_before_run=True):

        self.delete_job()

        if sync_to_s3_before_run:
            self.sync_job_to_s3_folder()

        job_definition = self._job_definition()
        _glue_client.create_job(**job_definition)

        response = _glue_client.start_job_run(
            JobName=self.job_name, Arguments=self.job_arguments
        )

        self._job_run_id = response["JobRunId"]

    @property
    def job_status(self):
        if self.job_run_id is None:
            raise JobNotStarted('Missing "job_run_id", have you started the job?')

        if self.job_name is None:
            raise JobMisconfigured('Missing "job_name"')

        return _glue_client.get_job_run(JobName=self.job_name, RunId=self.job_run_id)

    @property
    def job_run_state(self):
        status = self.job_status
        return status["JobRun"]["JobRunState"]

    @property
    def is_running(self):
        return self.job_run_state == "RUNNING"

    def wait_for_completion(
        self,
        verbose=False,
        wait_seconds=10,
        back_off_retries=5,
        cleanup_if_successful=False,
    ):
        """
        Wait for the job to complete.

        This means it either succeeded or it was manually stopped.

        Raises:
            JobFailed: When the job failed
            JobTimedOut: When the job timed out
        """

        back_off_counter = 0
        while True:
            time.sleep(wait_seconds)

            try:
                status = self.job_status
            except ClientError as e:
                if (
                    "ThrottlingException" in str(e)
                    and back_off_counter < back_off_retries
                ):
                    back_off_counter += 1
                    back_off_wait_time = wait_seconds * (2 ** (back_off_counter))
                    status_code = (
                        f"BOTO_CLIENT_RATE_EXCEEDED (waiting {back_off_wait_time}s)"
                    )
                    time.sleep(back_off_wait_time)
                else:
                    if "ThrottlingException" in str(e):
                        err_str = f"Total number of retries ({back_off_retries}) exceeded - {str(e)}"
                        raise JobThrottlingExceeded(err_str)
                    else:
                        raise e
            else:
                back_off_counter = 0
                status_code = status["JobRun"]["JobRunState"]
                status_error = status["JobRun"].get("ErrorMessage", "n/a")
                exec_time = status["JobRun"].get("ExecutionTime", "n/a")

            if verbose:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(
                    (
                        f"{timestamp}: Job State: {status_code} | "
                        f"Execution Time: {exec_time} (s) | Error: {status_error}"
                    )
                )

            if status_code == "SUCCEEDED":
                break

            if status_code == "FAILED":
                raise JobFailed(status_error)
            if status_code == "TIMEOUT":
                raise JobTimedOut(status_error)
            if status_code == "STOPPED":
                raise JobStopped(status_error)

        if status_code == "SUCCEEDED" and cleanup_if_successful:
            back_off_counter = 0
            if verbose:
                print("JOB SUCCEEDED: Cleaning Up")

            while True:
                try:
                    self.cleanup()
                except ClientError as e:
                    if (
                        "ThrottlingException" in str(e)
                        and back_off_counter < back_off_retries
                    ):
                        back_off_counter += 1
                        back_off_wait_time = wait_seconds * (2 ** (back_off_counter))
                        time.sleep(back_off_wait_time)
                    else:
                        if "ThrottlingException" in str(e):
                            err_str = f"Total number of retries ({back_off_retries}) exceeded - {str(e)}"
                            raise JobThrottlingExceeded(err_str)
                        else:
                            raise e
                else:
                    break

    def cleanup(self):
        """
        Delete the Glue Job resources (the job itself and the S3 objects)
        """

        self.delete_job()
        self.delete_s3_job_temp_folder()

    def delete_job(self):
        """
        DEPRECATED: Use `cleanup()`
        """

        if self.job_name is None:
            raise JobMisconfigured('Missing "job_name"')

        _glue_client.delete_job(JobName=self.job_name)

    def delete_s3_job_temp_folder(self):
        """
        DEPRECATED: Use `cleanup()`
        """

        bucket = _s3_resource.Bucket(self.bucket)
        bucket.objects.filter(Prefix=self.s3_job_folder_no_bucket).delete()
