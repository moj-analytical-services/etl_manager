from mojdbtemplate.utils import _read_json, _write_json, _dict_merge, _end_with_slash, _validate_string, _glue_client, _unnest_github_zipfile_and_return_new_zip_path, _s3_client, _get_file_from_file_path
from urllib.request import urlretrieve
import os 
import re
import json

# Create temp folder - upload to s3
# Lock it in 
# Run job - check if job exists and lock in temp folder matches 

class Glue_Job_Runner :
    """
    Take a folder structure on local disk and transfer to s3.

    Folder must be formatted as follows:
    base dir
      job.py
      glue_py_resources/
        zip and python files
        zip_urls <- file containing urls of additional zip files e.g. on github
      glue_resources/
        txt, sql, json, or csv files

    The folder name base dir will be in the folder s3_path_to_glue_jobs_folder
    """
    def __init__(self, glue_job_folder, s3_bucket, job_role, job_suffix = '_dev', include_shared_job_resources = True) :

        if not os.path.exists(self.glue_job_folder + 'job.py') :
            raise ValueError("Could not find job.py in base directory provided ({}), stopping.\nOnly folder allowed to have no job.py is a folder named shared_job_resources".format(glue_job_folder))

        glue_job_folder_split = glue_job_folder.split('/')

        self.glue_job_folder = glue_job_folder
        self.s3_bucket = s3_bucket
        self.job_folder = glue_job_folder_split[-1]
        self.job_name = glue_job_folder_split[-1]
        self.job_role = job_role
        self.py_resources = self._get_resources(True, include_shared_job_resources)
        self.resources = self._get_resources(False, include_shared_job_resources)
        self.github_zip_urls = self._get_github_resource_list(include_shared_job_resources)
        
        self.s3_job_response = None
        self._lockin_upload_path = None

        # Set as default can change using standard getters and setters
        self.max_retries = 0
        self.max_concurrent_runs = 1
        self.allocated_capacity = 2

    @property
    def glue_job_folder(self) :
        return self._glue_job_folder

    @glue_job_folder.setter
    def glue_job_folder(self, glue_job_folder) :
        glue_job_folder = _end_with_slash(glue_job_folder)
        self._glue_job_folder = glue_job_folder

    @property
    def s3_job_folder(self) :
        return "s3://{}/{}/{}/{}/".format(self.s3_bucket, '_temp_glue_job_runner_', self.job_name, 'resources')

    #  @property
    # def s3_job_temp_folder(self) :
    #     return "s3://{}/{}/{}/{}/".format(self.s3_bucket, '_temp_glue_job_runner_', self.job_name, 'temp_dir')

    def _glue_job_folder_parent(self) :
        parent = self.glue_job_folder.split('/')
        return '/'.join(parent[:-2]) + '/'
    
    def _check_nondup_resources(self, resources_list) :
        file_list = [r.split('/')[-1] for r in resources_list]
        if(len(file_list) != len(set(file_list))) :
            raise ValueError("There are duplicate file names in your suplied resources. A file in job resources might share the same name as a file in the shared resources folders.")
            
    def _get_github_resource_list(self, include_shared_job_resources) :
        zip_urls_path = os.path.join(self.glue_job_folder, "py_resources", "github_zip_urls.txt")
        shared_zip_urls_path = os.path.join(self.glue_job_folder, "shared_job_resources", "py_resources", "github_zip_urls.txt")

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
        
        resources_path = os.path.join(self.glue_job_folder, resource_folder)
        shared_resources_path = os.path.join(self._glue_job_folder_parent(), "shared_job_resources", resource_folder)

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

    def sync_job_to_s3_folder(self) :
        # Upload all the .py or .zip files in resources
        # Check existence of folder, otherwise skip
        files_to_sync = []
        for r in self.resources :
            files_to_sync.append(r)

        for r in self.py_resources :
            files_to_sync.append(r)

        delete_these_paths = []
        if len(self.github_zip_urls) > 0 :
            for i, url in enumerate(self.github_zip_urls) :
                zip_name = "{}.zip".format(i)
                
                # Check downloaded zip doesn't conflict with any py_resources
                if any([zip_name in name for name in self.py_resources]) :
                    raise ValueError("This github zip file ({}) already exists in your py_resources".format(zip_name))
                
                this_zip_path = os.path.join(self.glue_job_folder, "_temp_git_zips", zip_name)
                urlretrieve(url, this_zip_path)
                new_zip_path = _unnest_github_zipfile_and_return_new_zip_path(this_zip_path)
                os.remove(this_zip_path)
                files_to_sync.append(new_zip_path)
                delete_these_paths.append(new_zip_path)

        for f in files_to_sync :
            _s3_client.upload_file(f, self.s3_bucket, self.s3_job_folder + _get_file_from_file_path(f))

        for f in delete_these_paths :
            os.remove(f)

        self._lockin_upload_path = self.s3_job_folder
        
        def run_job(self) :
            if self._lockin_upload_path != self.s3_job_folder :
                self.sync_job_to_s3_folder()
            
            _glue_client.start_job()

            "something"

        def _create_glue_job_definition(self, **kwargs):

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
                "MaxRetries": 0,
                "AllocatedCapacity": 2
            }

            template["Name"] = self.job_name
            template["Role"] = self.job_role
            template["Command"]["ScriptLocation"] = self.s3_job_folder + 'job.py'
            template["DefaultArguments"]["--TempDir"] = self.s3_temp_folder

            if len(self.resources) > 0 :
                extra_files = ','.join([self.s3_temp_folder + _get_file_from_file_path(f) for f in self.resources])
                template["DefaultArguments"]["--extra-files"] = extra_files
            else :
                template["DefaultArguments"].pop("--extra-files", None)

            if len(self.py_resources) > 0 :
                extra_py_files = ','.join([self.s3_temp_folder + _get_file_from_file_path(f) for f in self.py_resources])
                template["DefaultArguments"]["--extra-py-files"] = extra_py_files
            else :
                template["DefaultArguments"].pop("--extra-py-files", None)

            template["MaxRetries"] = self.max_retries
            template["ExecututionProperty"]["MaxConcurrentRuns"] = self.max_concurrent_runs
            template["AllocatedCapacity"] = self.allocated_capacity

            return template