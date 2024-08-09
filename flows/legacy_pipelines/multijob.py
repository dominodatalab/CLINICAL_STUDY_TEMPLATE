import configparser
import os
import sys
import time
import pprint
import logging
import requests
from requests.exceptions import HTTPError

"""
on each tick:
- iterate through list of tasks and check status of dependencies.
- return a list of tasks that are elibible to be submitted.
- submit eligible runs
"""


logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

import json
from time import sleep
from datetime import datetime

DOMINO_RUN_ID = os.environ['DOMINO_RUN_ID']
DOMINO_STARTING_USERNAME = os.environ['DOMINO_STARTING_USERNAME']
DOMINO_API_HOST = os.environ['DOMINO_API_PROXY']
DOMINO_PROJECT_ID = os.environ['DOMINO_PROJECT_ID']
DOMINO_PROJECT_NAME = os.environ['DOMINO_PROJECT_NAME']
DOMINO_IS_GIT_BASED = os.environ['DOMINO_IS_GIT_BASED']

# These variables may not be set in the project, which we should interpret as a 'false' value
try:
    CXRUN = os.environ['DMV_ISCX'].lower()
except KeyError:
    CXRUN = 'false'

try:
    PRERUN_CLEANUP = os.environ['DMV_PREP'].lower()
except KeyError:
    PRERUN_CLEANUP = 'false'

class DominoRun:
    """
    self.task_id        # name of task
    self.command        # command to submit to API
    self.isDirect       # isDirect flag to submit to API
    self.max_retries    # maximum retries

    self.job_id        # ID of latest run attempt
    self.retries        # number of retries so far
    self.status()       # check API for status - stop checking once Succeeded or (Error/Failed and self.retries < self.max_retries)
    self._status        # last .status()
    
    once submitted, it polls status, and retries (submits re-runs) up to max_retries
    """
    def __init__(self, task_id, command, max_retries=0, tier=None, environment=None, project_repo_git_ref=None, imported_repo_git_refs=None):
        self.task_id = task_id
        self.command = command
        self.max_retries = max_retries
        self.tier = tier
        self.environment = environment
        self.project_repo_git_ref = project_repo_git_ref
        self.imported_repo_git_refs = imported_repo_git_refs
        self.job_id = None
        self.retries = 0
        self._status = "Unsubmitted"

    def status(self):
        if self._status not in ("Succeeded", "Unsubmitted", "Error", "Failed", "Stopped"):
            job_status = get_job_status(self.job_id)
            self.set_status(job_status)
        return self._status

    def set_status(self, status):
        self._status = status

    def is_complete(self):
        return self.status() == "Succeeded"


class Dag:
    """
    self.tasks              # dictionary of task_ids -> DominoRun objects
    self.dependency_graph   # dictionary of task_ids -> list of dependency task_ids
    """
    def __init__(self, tasks, dependency_graph, allow_partial_failure=False):
        self.tasks = tasks
        self.dependency_graph = dependency_graph
        self.allow_partial_failure = allow_partial_failure

    def get_dependency_statuses(self, task_id):
        dependency_statuses = []
        deps = self.dependency_graph[task_id]
        for dep in deps:
            dependency_statuses += [self.tasks[dep].status()]
        return dependency_statuses

    def are_task_dependencies_complete(self, task_id):
        dependency_statuses = self.get_dependency_statuses(task_id)
        if dependency_statuses:
            all_deps_succeeded = all(status == 'Succeeded' for status in dependency_statuses)
        else:
            all_deps_succeeded = True
        return all_deps_succeeded

    def get_ready_tasks(self):
        ready_tasks = []
        for task_id, task in self.tasks.items():
            deps_complete = self.are_task_dependencies_complete(task_id)
            task_status_ready = (task.status() in ("Error", "Failed") and task.retries < task.max_retries) or task.status() == 'Unsubmitted'
            if deps_complete and task_status_ready:
                ready_tasks.append(task)
        return ready_tasks

    def get_failed_tasks(self):
        failed_tasks = []
        for task_id, task in self.tasks.items():
            if task.status() in ('Error', 'Failed') and task.retries >= task.max_retries:
                failed_tasks.append(task)
        return failed_tasks

    def pipeline_status(self):
        status = 'Running'
        if len(self.get_failed_tasks()) > 0 and self.allow_partial_failure == False:
            status = 'Failed'
        elif all(task.is_complete() for task_id, task in self.tasks.items()):
            status = 'Succeeded'
        return status

    def validate_dag(self):
        def recurse(task, original_task):
            dag_valid = True
            for dependency in self.dependency_graph[task]:
                if original_task in self.dependency_graph[dependency]:
                    dag_valid = False
                    print(f"ERROR: Circular dependency detected. {original_task} won't start because {task} depends on {dependency}, but {dependency} depends on {original_task}.\nPlease review your config and resolve any circular references.")
                if dag_valid == False:
                    break
                else:
                    dag_valid = recurse(dependency, original_task)
        
            return dag_valid

        for task in self.dependency_graph:
            dag_valid = recurse(task, task)
        
            if not dag_valid:
                print('ERROR: Exiting due to invalid dependency structure.')
                exit(1)

    def validate_run_command(self):
        pass

    def __str__(self):
        return pprint.pformat(self.dependency_graph, width=1)
            
def submit_api_call(method, endpoint, data=None):
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
    }
    url = f'{DOMINO_API_HOST}/{endpoint}'

    try:
        response = requests.request(method, url, headers=headers, data=data)
        response.raise_for_status()
    except HTTPError as err:
        print(err)
        if data:
            print(f'Request Body: {data}')
        print(f'Request Response: {response.text}')
        exit(1)

    # Some API responses have JSON bodies, some are empty
    try:
        return response.json()
    except:
        try:
            return response.text
        except:
            return response


def get_job_status(job_id):
    endpoint = f'api/jobs/beta/jobs/{job_id}'
    method = 'GET'
    job_information = submit_api_call(method, endpoint)
    job_status = job_information['job']['status']['executionStatus']

    return job_status

def get_project_datasets():
    endpoint = f'api/datasetrw/v2/datasets?projectIdsToInclude={DOMINO_PROJECT_ID}'
    method = 'GET'
    project_datasets = submit_api_call(method, endpoint)

    return project_datasets


def take_dataset_snapshot(dataset_id):
    endpoint = f'api/datasetrw/v1/datasets/{dataset_id}/snapshots'
    method = 'POST'
    data = { "relativeFilePaths":["."] }
    snapshot_response = submit_api_call(method, endpoint, data=json.dumps(data))

    snapshot_id = snapshot_response['snapshot']['id']
    snapshot_timestamp = snapshot_response['snapshot']['createdAt']
    dt = datetime.strptime(snapshot_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    formatted_timestamp = str(dt.strftime('D%d-%b-%Y-T%H-%M-%S'))

    snapshot_status = ''
    while snapshot_status.lower() != 'active':
        sleep(2)
        endpoint = f'api/datasetrw/v1/snapshots/{snapshot_id}'
        method = 'GET'
        snapshot_status_response = submit_api_call(method, endpoint)
        snapshot_status = snapshot_status_response['snapshot']['status']

    return snapshot_id, formatted_timestamp, snapshot_response


def tag_dataset_snapshot(dataset_id, snapshot_id, formatted_timestamp):
    endpoint = f'api/datasetrw/v1/datasets/{dataset_id}/tags'
    method = 'POST'
    # {"message":"Tags must start with a letter and contain only alphanumeric, dashes, and hyphen characters","success":false}
    job_tag = f'JOB{DOMINO_RUN_ID}'
    tags = [ formatted_timestamp, job_tag ]
    for tag in tags:
        data = { "snapshotId": snapshot_id, "tagName": tag }
        tag_response = submit_api_call(method, endpoint, data=json.dumps(data))


def format_snapshot_comment(snapshot_response, formatted_timestamp):
    snapshot_json = snapshot_response
    dataset_id = snapshot_json['snapshot']['datasetId']

    endpoint = f'api/datasetrw/v1/datasets/{dataset_id}'
    method = 'GET'
    dataset_response = submit_api_call(method, endpoint)

    dataset_name = dataset_response['dataset']['name']
    snapshot_comment = \
        f"Controlled execution results snapshot:\\\n\\\n \
            Dataset ID: {snapshot_json['snapshot']['datasetId']}\\\n \
            Dataset name: {dataset_name}\\\n \
            Author MUD ID: {DOMINO_STARTING_USERNAME}\\\n \
            Creation time: {formatted_timestamp}"

    return snapshot_comment


def format_env_vars_comment():
    variables_comment = 'Project environment variables:\\\n'
    for env_var in os.environ:
        if env_var.startswith('DMV'):
            variables_comment += f'\\\n{env_var}: {os.environ[env_var]}'
    
    return variables_comment


def leave_comment_on_job(comment_text):
    endpoint = f'v4/jobs/{DOMINO_RUN_ID}/comment'
    method = 'POST'
    data = { "comment": comment_text }
    comment_response = submit_api_call(method, endpoint, data=json.dumps(data))


def cleanup_datasets():
    project_datasets = get_project_datasets()
    if DOMINO_IS_GIT_BASED == 'true':
        dataset_root = '/mnt/data'
    else:
        dataset_root = '/domino/datasets/local'
    for dataset in project_datasets['datasets']:
        dataset_path = f"{dataset_root}/{dataset['dataset']['name']}"
        PROTECTED_DIR = 'inputdata'
        for (root, dirs, files) in os.walk(dataset_path, topdown=True):
            for name in files:
                if PROTECTED_DIR not in root:
                    os.remove(os.path.join(root, name))


def full_cx():
    project_datasets = get_project_datasets()
    for dataset in project_datasets['datasets']:
        dataset_id = dataset['dataset']['id']
        snapshot_id, formatted_timestamp, snapshot_response = take_dataset_snapshot(dataset_id)
        tag_dataset_snapshot(dataset_id, snapshot_id, formatted_timestamp)
        snapshot_comment = format_snapshot_comment(snapshot_response, formatted_timestamp)
        leave_comment_on_job(snapshot_comment)

    variables_comment = format_env_vars_comment()
    leave_comment_on_job(variables_comment)



def build_dag(cfg_file_path):
    c = configparser.ConfigParser(allow_no_value=False)
    c.read(cfg_file_path)
    tasks = {}
    dependency_graph = {}
    task_ids = c.sections()
    if len(task_ids) == 0:
        raise Exception("Empty config provided")
    for task_id in task_ids:
        if c.has_option(task_id, "depends"):
            dependencies_str = c.get(task_id, "depends")
            dependencies = dependencies_str.split(',')
        else:
            dependencies = []
        dependency_graph[task_id] = dependencies
        command_str = c.get(task_id, "command")
        command = str(command_str)
        domino_run_kwargs = {}
        if c.has_option(task_id, "max_retries"):
            max_retries = c.get(task_id, "max_retries")
            domino_run_kwargs["max_retries"] = max_retries
        if c.has_option(task_id, "tier"):
            tier = c.get(task_id, "tier")
            domino_run_kwargs["tier"] = tier
        # Set the desired compute environment
        if c.has_option(task_id, "environment"):
            environment = c.get(task_id, "environment")
            domino_run_kwargs["environment"] = environment
        if c.has_option(task_id, 'project_repo_git_ref'):
            project_repo_git_ref = c.get(task_id, 'project_repo_git_ref')
            domino_run_kwargs['project_repo_git_ref'] = project_repo_git_ref
        if c.has_option(task_id, 'imported_repo_git_refs'):
            imported_repo_git_refs = c.get(task_id, 'imported_repo_git_refs')
            domino_run_kwargs['imported_repo_git_refs'] = imported_repo_git_refs
        tasks[task_id] = DominoRun(task_id, command, **domino_run_kwargs)
    
    return Dag(tasks, dependency_graph)



class PipelineRunner:
    '''
    should this be stateless or stateful?
    - needs to be stateful to track run IDs and states (for retry logic) of various tasks
    - use Dag object to store state
    '''

    def __init__(self, dag, tick_freq=5):
        self.dag = dag
        self.tick_freq = tick_freq

    def run(self):
        while True:
            if self.dag.pipeline_status() == 'Succeeded':
                break
            elif self.dag.pipeline_status() == 'Failed':
                raise Exception("Pipeline Execution Failed")
            # Suspend job submission until the "multijob_locked" project tag is removed
            while True:
                jobs_locked = self.are_jobs_locked()
                if jobs_locked == False:
                    break
                time.sleep(self.tick_freq)
            # Suspend job submission until the number of jobs is below the queue limit
            while True:
                queue_limit = 10
                queued_job_count = self.check_queue_limit()
                if queued_job_count < queue_limit:
                    break
                print('At limit for queued jobs, waiting for queue space.')
                time.sleep(self.tick_freq)

            ready_tasks = dag.get_ready_tasks()
            if ready_tasks:
                print("Ready tasks: {0}".format(", ".join([task.task_id for task in ready_tasks])))
            # Pull one task out of the ready queue, submit it, wait 1 tick, and repeat tag check
            for task in ready_tasks:
                self.submit_task(task)
                break
            time.sleep(self.tick_freq)


    def get_hardware_tier_id(self, hardware_tier_name):
        endpoint = f'v4/projects/{DOMINO_PROJECT_ID}/hardwareTiers'
        method = 'GET'
        available_hardware_tiers = submit_api_call(method, endpoint)
        for hardware_tier in available_hardware_tiers:
            if hardware_tier['hardwareTier']['name'] == hardware_tier_name:
                hardware_tier_id = hardware_tier['hardwareTier']['id']

        return hardware_tier_id


    def set_project_tag(self):
        endpoint = f'v4/projects/{DOMINO_PROJECT_ID}/tags'
        method = 'POST'
        data = {
            'tagNames': [
                'multijob_locked'
            ]
        }
        set_tag_response = submit_api_call(method, endpoint, data=json.dumps(data))
        tag_id = set_tag_response[0]['id']

        return tag_id


    def delete_project_tag(self, tag_id):
        endpoint = f'v4/projects/{DOMINO_PROJECT_ID}/tags/{tag_id}'
        method = 'DELETE'
        delete_tag_response = submit_api_call(method, endpoint)


    def are_jobs_locked(self):
        jobs_locked = False
        endpoint = f'v4/projects/{DOMINO_PROJECT_ID}'
        method = 'GET'
        project_summary = submit_api_call(method, endpoint)

        if 'tags' in project_summary:
            for tag in project_summary['tags']:
                if tag['name'] == 'multijob_locked':
                    jobs_locked = True
                    break
    
        return jobs_locked


    def check_queue_limit(self):
        endpoint = f'api/jobs/beta/jobs?projectId={DOMINO_PROJECT_ID}&statusFilter=queued'
        method = 'GET'
        queued_jobs = submit_api_call(method, endpoint)

        queued_jobs_count = queued_jobs['metadata']['totalCount']

        return queued_jobs_count


    def get_imported_repos(self):
        endpoint = f'api/projects/v1/projects/{DOMINO_PROJECT_ID}/repositories'
        method = 'GET'
        imported_repos = submit_api_call(method, endpoint)
        
        return imported_repos

    # Imported repo configs are specified in 3 parts, delimited with commas.
    # The format is: repo_name,ref_type,ref_value
    # Multiple repo configs are delimited by spaces. For example:
    # imported_repo_git_refs: my-repo,branches,feature-branch other-repo,tags,tag-value

    # We need to store the user defined temporary config, as well as the config's starting state.
    # Builds 2 dicts, "original_config" from the current repo config pulled by the API,
    # and "temp_config" using the user-provided values in the .cfg file
    def build_imported_repo_configs(self, imported_repo_config):
        current_imported_repos = self.get_imported_repos()
        temp_config = { }
        original_config = { }
        # Multiple repos can be specified by delimiting entries with a space
        imported_repos_to_update = imported_repo_config.split(' ')
        for i, repo in enumerate(imported_repos_to_update, start=1):
            # Individual repo configs are delimited by commas. The first position is the repo name,
            # the second is the ref type, but there may not be a third, for example if the ref type is 'HEAD'.
            modified_repo_name, *modified_git_ref = repo.split(',')
            modified_ref_type = modified_git_ref[0]
            if len(modified_git_ref) == 2:
                modified_ref_value = modified_git_ref[1]
            for current_repo in current_imported_repos['repositories']:
                if current_repo['name'] == modified_repo_name:
                    temp_config[i] = {
                        'id': current_repo['id'],
                        'ref_type': modified_ref_type,
                    }
                    original_config[i] = {
                        'id': current_repo['id'],
                        'ref_type': current_repo['defaultRef']['refType']
                    }
                    if 'value' in current_repo['defaultRef']:
                        original_config[i]['ref_value'] = current_repo['defaultRef']['value']
                    if modified_ref_value is not None:
                        temp_config[i]['ref_value'] = modified_ref_value

        # The resulting dicts have the minimum required info to update the repo config for the project
        # and are formatted as such:
        # {
        #     '1': {
        #         'id': 'repo-uid',
        #         'ref_type': 'git-ref-type',
        #         'ref_value': 'value-of-that-ref'
        #     },
        #     '2': {
        #         'id': 'repo-uid',
        #         'ref_type': 'git-ref-type',
        #         'ref_value': 'value-of-that-ref'
        #     }
        # }
        return original_config, temp_config


    # Accepts the repo config dicts created by build_imported_repo_configs()
    def set_imported_repo_config(self, repo_config):
        for i in repo_config:
            repo_id = repo_config[i]['id']
            if repo_config[i]['ref_type'] == 'Head':
                ref_type = 'HEAD'
            else:
                ref_type = repo_config[i]['ref_type']
            if 'ref_value' in repo_config[i]:
                ref_value = repo_config[i]['ref_value']
            endpoint = f'v4/projects/{DOMINO_PROJECT_ID}/gitRepositories/{repo_id}/ref'
            method = 'PUT'
            git_ref_config = {
                'type': ref_type,
            }
            if 'ref_value' in repo_config[i]:
                git_ref_config['value'] = ref_value

            response = submit_api_call(method, endpoint, data=json.dumps(git_ref_config))


    def submit_task(self, task):
        print(f"## Submitting task ##\ntask_id: {task.task_id}\ncommand: {task.command}\ntier override: {task.tier}\nenvironment override: {task.environment}\nmain repo override: {task.project_repo_git_ref}\nimported repo overrides: {task.imported_repo_git_refs}")
        request_body = { 'projectId': DOMINO_PROJECT_ID }
        
        # R scripts should be wrapped in the logrx::axecute() function
        if task.command.lower().endswith('.r'):
            print('R script detected. Running via logrx::axecute().')
            if DOMINO_IS_GIT_BASED == 'true':
                dataset_root = '/mnt/data'
            else:
                dataset_root = '/domino/datasets/local'
            if os.path.exists(f'{dataset_root}/{DOMINO_PROJECT_NAME}'):
                logrx_log_path = f'{dataset_root}/{DOMINO_PROJECT_NAME}/logs/'
                if not os.path.exists(logrx_log_path):
                    os.makedirs(logrx_log_path)
                task.command = f'R -e "logrx::axecute(\'{task.command}\', log_path = \'{logrx_log_path}\')"'

        request_body['runCommand'] = task.command
        # If the user has specified custom git refs, set the "multijob_locked" tag before doing anything else
        # Then, save the current imported repo config to revert later before setting the user config.
        if task.imported_repo_git_refs:
            tag_id = self.set_project_tag()
            original_config, temp_config = self.build_imported_repo_configs(task.imported_repo_git_refs)
            self.set_imported_repo_config(temp_config)
        if task.tier:
            hardware_tier_id = self.get_hardware_tier_id(task.tier)
            request_body['hardwareTier'] = hardware_tier_id
        if task.environment:
            request_body['environmentId'] = task.environment
        if task.project_repo_git_ref:
            project_repo_config = task.project_repo_git_ref.split(',')
            if len(project_repo_config) == 2:
                request_body['mainRepoGitRef'] = { 'refType': project_repo_config[0], 'value': project_repo_config[1] }
            else:    
                request_body['mainRepoGitRef'] = { 'refType': project_repo_config[0] }

        endpoint = 'api/jobs/v1/jobs'
        method = 'POST'
        job_info = submit_api_call(method, endpoint, data=json.dumps(request_body))
        print(job_info)
        # Domino doesn't load the imported git repo config as part of the job submission.
        # Instead, it's loaded during job startup, which is the 'Preparing' state.
        # If using a custom git ref, Multijob should block until that job is Preparing.
        # Once the job is starting up, revert the git config and delete the "multijob_lock" tag.
        if task.imported_repo_git_refs:
            while True:
                job_state = get_job_status(job_info['job']['id'])
                if job_state not in ['Queued', 'Pending']:
                    break
                time.sleep(3)
            self.set_imported_repo_config(original_config)
            self.delete_project_tag(tag_id)

        print("## Submitted task: {0} ##".format(task.task_id))
        task.job_id = job_info['job']['id']
        task.set_status('Submitted') # will technically be Queued or something else, but this will update on the next status check



"""

v0.1: just read the cfg file and submit commands. skip all validation, put responsibility on end-user. assume jobs are idempotent.

"""

if __name__ == '__main__':
    pipeline_cfg_path = sys.argv[1]
    if os.path.exists(pipeline_cfg_path):
        if PRERUN_CLEANUP == 'true':
            cleanup_datasets()
        dag = build_dag(pipeline_cfg_path)
        print(dag)
        dag.validate_dag()
        pipeline_runner = PipelineRunner(dag)
        pipeline_runner.run()
        if CXRUN == 'true':
            full_cx()
    else:
        sys.exit("Empty or missing config file")
