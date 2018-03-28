from mojdbtemplate.utils import _read_json, _write_json, _dict_merge, _end_with_slash, _validate_string, _glue_client, _unnest_github_zipfile_and_return_new_zip_path, _s3_client, _get_file_from_file_path, _s3_resource
from urllib.request import urlretrieve
import os 
import re
import json
import tempfile
import zipfile
import shutil

# Create temp folder - upload to s3
# Lock it in 
# Run job - check if job exists and lock in temp folder matches 

class Glue_Job_Runner :
    """
    Take a folder structure on local disk.

    Folder must be formatted as follows:
    job_folder
      job.py
      glue_py_resources/
        zip and python files
        zip_urls <- file containing urls of additional zip files e.g. on github
      glue_resources/
        txt, sql, json, or csv files

    Can then run jobs on aws glue using this class.

    If include_shared_job_resources is True then glue_py_resources and glue_resources folders inside a special named folder 'shared_glue_resources'
    will also be referenced.
    glue_jobs (parent folder to 'job_folder')
      shared_glue_resources
        glue_py_resources/
          zip, python and zip_urls
        glue_resources/
          txt, sql, json, or csv files
      job_folder
        etc...
    """
    def __init__(self, job_folder, bucket, job_role, job_arguments = None, include_shared_job_resources = True) :

        self._job_folder = job_folder

        if not os.path.exists(self.job_folder + 'job.py') :
            raise ValueError("Could not find job.py in base directory provided ({}), stopping.\nOnly folder allowed to have no job.py is a folder named shared_job_resources".format(job_folder))

        glue_job_folder_split = self.job_folder.split('/')

        self.bucket = bucket
        self.job_name = glue_job_folder_split[-2]
        self.job_role = job_role
        self.py_resources = self._get_resources(True, include_shared_job_resources)
        self.resources = self._get_resources(False, include_shared_job_resources)
        self.github_zip_urls = self._get_github_resource_list(include_shared_job_resources)
        self.job_arguments = job_arguments

        self.github_py_resources = []
        # self.external_py_resources = None
        # self.external_resources = None

        # Set as default can change using standard getters and setters
        self.max_retries = 0
        self.max_concurrent_runs = 1
        self.allocated_capacity = 2

    @property
    def job_folder(self) :
        return self._job_folder

    @property
    def s3_job_folder(self) :
        return "s3://{}/{}".format(self.bucket, self.s3_job_folder_obj)
        
    @property
    def s3_job_folder_obj(self) :
        return "{}/{}/{}/".format('_temp_glue_job_runner_', self.job_name, 'resources')

    @property
    def job_parent_folder(self) :
        parent = self.job_folder.split('/')
        return '/'.join(parent[:-2]) + '/'
    
    @property
    def job_arguments(self) :
        return self._job_arguments

    @job_arguments.setter
    def job_arguments(self, job_arguments) :
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        if job_arguments is not None :
            if not isinstance(job_arguments, dict) :
                raise ValueError("job_arguments must be a dictionary")
            # validate dict keys    
            special_aws_params = ['--JOB_NAME', '--conf', '--debug', '--mode']
            for k in job_arguments.keys() :
                if k[:2] != '--' or k in special_aws_params:
                    raise ValueError("Found incorrect AWS job argument ({}). All arguments should begin with '--' and cannot be one of the following: {}".format(k, ', '.join(special_aws_params)))
        self._job_arguments = job_arguments
    
    @property
    def bucket(self) :
        return self._bucket

    @bucket.setter
    def bucket(self, bucket) :
        _validate_string(bucket, '-,')
        self._bucket = bucket

    def _check_nondup_resources(self, resources_list) :
        file_list = [_get_file_from_file_path(r) for r in resources_list]
        if(len(file_list) != len(set(file_list))) :
            raise ValueError("There are duplicate file names in your suplied resources. A file in job resources might share the same name as a file in the shared resources folders.")
            
    def _get_github_resource_list(self, include_shared_job_resources) :
        zip_urls_path = os.path.join(self.job_folder, "glue_py_resources", "github_zip_urls.txt")
        shared_zip_urls_path = os.path.join(self.job_parent_folder, "shared_job_resources", "glue_py_resources", "github_zip_urls.txt")

        if os.path.exists(zip_urls_path) :
            with open(zip_urls_path, "r") as f:
                urls = f.readlines()
            f.close()
        else :
            urls = []

        if os.path.exists(shared_zip_urls_path) and include_shared_job_resources :
            with open(shared_zip_urls_path, "r") as f:
                shared_urls = f.readlines()
            f.close()
            urls = urls + shared_urls 
        
        urls = [url for url in urls if len(url) > 10]

        return urls

    def _get_resources(self, py, include_shared_job_resources) :
        # Upload all the .py or .zip files in resources
        # Check existence of folder, otherwise skip
        
        resource_folder = "glue_py_resources" if py else "glue_resources"
        regex = ".+(\.py|\.zip)$" if py else ".+(\.sql|\.json|\.csv|\.txt)$"
        
        resources_path = os.path.join(self.job_folder, resource_folder)
        shared_resources_path = os.path.join(self.job_parent_folder, "shared_job_resources", resource_folder)

        if os.path.isdir(resources_path) :
            resource_listing = os.listdir(resources_path)
            resource_listing = [_end_with_slash(resources_path) + f for f in resource_listing if re.match(regex, f)]
        else :
            resource_listing = []
        
        if os.path.isdir(shared_resources_path) and include_shared_job_resources :
            shared_resource_listing = os.listdir(shared_resources_path)
            shared_resource_listing = [_end_with_slash(shared_resources_path) + f for f in shared_resource_listing if re.match(regex, f)]
            resource_listing = resource_listing + shared_resource_listing

        return resource_listing
        
    def _download_github_zipfile_and_rezip_to_glue_file_structure(self, url) :
        
        this_zip_path = os.path.join('_tmp_zip_files_to_s3_',"github.zip")
        urlretrieve(url, this_zip_path)

        original_dir = os.path.dirname(this_zip_path)

        with tempfile.TemporaryDirectory() as td:
            myzip = zipfile.ZipFile(this_zip_path, 'r')
            myzip.extractall(td)
            nested_folder_to_unnest = os.listdir(td)[0]
            nested_path = os.path.join(td, nested_folder_to_unnest)
            name = [d for d in os.listdir(nested_path) if os.path.isdir(os.path.join(nested_path, d))][0]
            output_path = os.path.join(original_dir, name)
            final_output_path = shutil.make_archive(output_path, 'zip', nested_path)

        os.remove(this_zip_path)

        return final_output_path

    def sync_job_to_s3_folder(self) :

        # Test if folder exists and create if not
        temp_folder_already_exists = False
        temp_zip_folder = '_tmp_zip_files_to_s3_'
        if os.path.exists(temp_zip_folder) :
            temp_folder_already_exists = True
        else :
            os.makedirs(temp_zip_folder)
        
        # Download the github urls and rezip them to work with aws glue
        self.github_py_resources = []
        for url in self.github_zip_urls :
            self.github_py_resources.append(self._download_github_zipfile_and_rezip_to_glue_file_structure(url))

        # Check if all filenames are unique
        files_to_sync = self.github_py_resources + self.py_resources + self.resources + [self.job_folder + 'job.py']
        self._check_nondup_resources(files_to_sync)

        # Sync all files to the same s3 folder
        for f in files_to_sync :
            s3_file_path = self.s3_job_folder_obj + _get_file_from_file_path(f)
            _s3_client.upload_file(f, self.bucket, s3_file_path)

        # Clean up downloaded zip files
        for f in list(self.github_py_resources) :
            os.remove(f)
        if not temp_folder_already_exists :
            os.rmdir(temp_zip_folder)

    
    def _create_glue_job_definition(self):

        template = {
            "Name": "",
            "Role": "",
            "ExecutionProperty": {
                "MaxConcurrentRuns": 1
            },
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": ""
            },
            "DefaultArguments": {
                "--TempDir": "",
                "--extra-files": "",
                "--extra-py-files": "",
                "--job-bookmark-option": "job-bookmark-disable"
            },
            "MaxRetries": None,
            "AllocatedCapacity": None
        }

        template["Name"] = self.job_name
        template["Role"] = self.job_role
        template["Command"]["ScriptLocation"] = self.s3_job_folder + 'job.py'
        template["DefaultArguments"]["--TempDir"] = self.s3_job_folder + 'glue_temp_folder/'

        if len(self.resources) > 0 :
            extra_files = ','.join([self.s3_job_folder + _get_file_from_file_path(f) for f in self.resources])
            template["DefaultArguments"]["--extra-files"] = extra_files
        else :
            template["DefaultArguments"].pop("--extra-files", None)

        if len(self.py_resources) > 0 :
            extra_py_files = ','.join([self.s3_job_folder + _get_file_from_file_path(f) for f in (self.py_resources + self.github_py_resources)])
            template["DefaultArguments"]["--extra-py-files"] = extra_py_files
        else :
            template["DefaultArguments"].pop("--extra-py-files", None)

        template["MaxRetries"] = self.max_retries
        template["ExecutionProperty"]["MaxConcurrentRuns"] = self.max_concurrent_runs
        template["AllocatedCapacity"] = self.allocated_capacity

        return template

    def run_job(self, sync_to_s3_before_run = True) :
        if sync_to_s3_before_run :
            self.sync_job_to_s3_folder()
        
        job_spec = self._create_glue_job_definition()

        self.delete_job()
        create_job_response = _glue_client.create_job(**job_spec)

        if self.job_arguments :
            response = _glue_client.start_job_run(JobName = self.job_name, Arguments = self.job_arguments)
        else:
            response = _glue_client.start_job_run(JobName = self.job_name)
        
        self.job_run_id = response['JobRunId']

    def get_job_status(self) :
        if self.job_name is None or self.job_run_id is None :
            response = "Object missing job_name or job_run_id"
        else :
            try :
                response = _glue_client.get_job_run(JobName = self.job_name, RunId = self.job_run_id)
            except :
                response = 'job name and job_run_id not found'
        return response

    def delete_job(self) :
        if self.job_name is None :
            raise ValueError("Object has no job_name.")

        try :
            _glue_client.delete_job(JobName=self.job_name)
        except :
            pass

    def delete_s3_job_temp_folder(self) :
        bucket = _s3_resource.Bucket(self.bucket)
        bucket.objects.filter(Prefix=self.s3_job_folder_obj).delete()