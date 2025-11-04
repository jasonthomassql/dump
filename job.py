import copy
import json
from typing import Any, Optional

# from databricks_toolbox import Job, JobDict # Inference from job.py.1.jpg line 9
# from utils.util import * # Inference from job.py.1.jpg line 10

CURRENT_ENV = 'ENV'
JOB_TEMPLATE_FILE = 'job_template.json'
TASK_TEMPLATE_FILE = 'task_template.json'
# CLUSTER_TEMPLATE_FILE = 'cluster_template.json' # Inference from job.py.2.jpg line 92

def create_env_config(job_config: dict, env: str) -> dict[str, Any]:
    """
    Generates environment-specific job configuration by merging 'standard' config
    with overrides from the target environment (e.g., 'dev', 'uat', 'prod').
    
    Args:
        job_config (dict): The dictionary which has 'standard' and 'env' specific configuration.
        env (str): The target environment for which the configuration is being generated.
        
    Returns:
        dict: A dictionary representing the merged configuration for the target environment.
    """
    
    standard_config = job_config.get("standard", {})
    env_config = job_config.get(env, {})

    # Creating 'schedule' by adding additional fields from environment specific config
    standard_config_schedule = standard_config.get("schedule", {})
    env_config_schedule = env_config.get("schedule", {})
    target_schedule = merge_nested_dict(standard_config_schedule, env_config_schedule)
    
    # Creating 'clusters' by adding additional fields from environment specific config
    standard_config_clusters = standard_config.get("job_clusters", [])
    env_config_clusters = env_config.get("job_clusters", [])
    
    target_clusters = standard_config_clusters.copy()
    # The first cluster is the job cluster root, this will always be populated and for the sake of this project, we wouldn't have multiple clusters
    if standard_config_clusters and env_config_clusters:
        target_clusters.pop(0)
        merged_cluster = merge_nested_dict(standard_config_clusters[0], env_config_clusters[0])
        target_clusters.insert(0, merged_cluster)
        
    elif standard_config_clusters:
        target_clusters = standard_config_clusters.copy()
        
    elif env_config_clusters:
        target_clusters.append(env_config_clusters[0])


    # Creating 'parameters' by adding additional fields from environment specific config
    standard_config_parameters = standard_config.get("parameters", [])
    env_config_parameters = env_config.get("parameters", [])
    
    target_config_parameters_dict = {param['name']: param for param in standard_config_parameters}
    env_config_parameters_dict = {param['name']: param for param in env_config_parameters}
    
    target_config_parameters_dict = merge_nested_dict(target_config_parameters_dict, env_config_parameters_dict)

    # For names not in target_config_parameters_dict (i.e. if a parameter exists in env but not standard)
    for name, default in target_config_parameters_dict.items():
        if name not in [param.get("name") for param in standard_config_parameters]:
             target_config_parameters_dict[name]['default'] = default['default']
    
    target_config_parameters = list(target_config_parameters_dict.values())
    
    # Final merged environment-specific config
    target_env_config = {
        "name": job_config.get("name"),
        "schedule": target_schedule,
        "job_clusters": target_clusters,
        "parameters": target_config_parameters,
        "tasks": standard_config.get("tasks", []) 
        # A task config remains same across the environments (inference)
    }
    
    return target_env_config

class Job:
    """
    Custom wrapper around Databricks Jobs allowing for easy construction of job definitions
    """
    
    # Load templates (Inference from job.py.2.jpg lines 92-94)
    # default_cluster_template = load_template_file(DEFAULT_NEW_CLUSTER_TEMPLATE_JSON) 
    # default_task_template = load_template_file(DEFAULT_TASK_TEMPLATE_JSON)
    # default_job_template = load_template_file(DEFAULT_JOB_TEMPLATE_JSON)

    def __init__(self, job_dict: dict, env: str = "dev"):
        self.name = job_dict.get("name")
        self.job_clusters = job_dict.get("job_clusters", list[dict[str, Any]])
        self.tasks = job_dict.get("tasks", list[dict[str, Any]])
        self.parameters = job_dict.get("parameters", list[dict[str, Any]])
        self.schedule = job_dict.get("schedule", Optional[dict]) or None
        
        # Super init call is assumed around line 105
        # super().__init__(job_dict, env) 
        
        self.self_custom_name = self.name
        self.self_custom_parameters = self.parameters
        self.self_job_clusters_overrides = self.job_clusters
        self.self_task_overrides = self.tasks
        self.self_task_keys = []
        self.self_job_cluster_keys = []


    def _build_job_clusters(self, job_clusters: list[dict[str, Any]], job_clusters_overrides: list[dict[str, Any]]) -> list:
        formatted_clusters_list = []
        clusters_to_process = job_clusters.copy()
        
        # Get 'new_cluster' for c in config_job_clusters
        # config_job_clusters_list = job_clusters.get('job_clusters', []) # Assuming this is around line 116
        
        for c in clusters_to_process:
            # Merge default templates with provider config if any, otherwise use default
            merged_cluster = merge_nested_dict(default_cluster_template, c) # Assuming default_cluster_template is available
            
            job_cluster_key = self._build_job_cluster_key(merged_cluster.get("node_type_id")) # Assuming node_type_id is used
            
            # Save it for later use in tasks
            self.self_job_cluster_keys.append(job_cluster_key)
            formatted_clusters_list.append({"job_cluster_key": job_cluster_key, "new_cluster": merged_cluster})
            
        return formatted_clusters_list
    
    
    def _build_job_cluster_key(self, node_type: str) -> str:
        type_name, node_name = node_type.split('.')
        type_prefix = type_name.split('-')[0] # 'i' from i3.xlarge
        # Using self_custom_name for unique key generation
        cluster_key = f"{self.self_custom_name}-{type_prefix}-xlrge" # Inference
        return cluster_key

    
    def _build_tasks(self, config_tasks: list[dict[str, Any]], config_tasks_overrides: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # This is where we will build the actual run_task config that is passed to the API
        formatted_tasks_list = []
        tasks_to_process = config_tasks.copy() # config_tasks is passed from job_template.json
        
        for task in tasks_to_process:
            # Merge default templates with provider config if any, otherwise use default
            merged_task = merge_nested_dict(default_task_template, task) # Assuming default_task_template is available
            
            # Use the first job cluster key - this will always be populated and for the sake of this project, we wouldn't have multiple clusters
            merged_task['job_cluster_key'] = self.self_job_cluster_keys[0]
            
            # The notebook path is dependent on the service principal
            if "sp" in self.self_custom_name:
                merged_task['notebook_task']['notebook_path'] = f"/resources/p1/sp/datasync/{self.self_custom_name}.ipynb"
            elif "elf_funds" in self.self_custom_name:
                merged_task['notebook_task']['notebook_path'] = f"/resources/p1/elf/fund-data/ctf/{self.self_custom_name}.ipynb"
            
            # Continued from job.py.4.jpg line 157
            elif 'elf' in self.job_template['tags'] and self.job_template['type'] == 'notebook':
                merged_task_notebook_path = f"/resources/p1/elf-fund-data/notebooks/{self.self_custom_name}.ipynb"
            else:
                merged_task_notebook_path = f"/resources/p1/legacy/datasync-ext/{self.self_custom_name}.ipynb"

            merged_task['notebook_task']['notebook_path'] = merged_task_notebook_path
            
            # Formatted tasks list appending and return (Inference)
            formatted_tasks_list.append(merged_task)
            
        return formatted_tasks_list

    
    def _validate_and_build_parameters(self, job_config_params: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not job_config_params:
            raise ValueError("**Parameters must be provided for the custom job.**")

        expected_params_not_src = (
            'external_location', 
            'schema_permissions', 
            'source_tables' # Reordered for better logic grouping
        )
        expected_params_hudi_elf_funds = (
            'external_location', 
            'schema_permissions', 
            'hudi_elf_funds'
        )
        expected_params_ext_src = (
            'external_location', 
            'schema_permissions', 
            'ext_src'
        )
        
        params = job_config_params.copy()
        params_names = [p['name'] for p in params]
        params_copy = params.copy() # Line 181
        
        for param in params:
            if 'name' not in param or 'default' not in param:
                raise ValueError("List parameter must have a 'name' and a 'default'.")
            
            # Converting 'source_tables' list parameter passed in job_config to desired str
            if param['name'] == 'source_tables':
                if isinstance(param['default'], list):
                    param['default'] = json.dumps(param['default'])
                elif not isinstance(param['default'], str):
                    raise ValueError(
                        "The 'source_tables' parameter must be a list or a JSON string."
                    )
            
            # Converting 'external_location' dict parameter passed in job_config to desired str
            if param['name'] == 'external_location':
                if isinstance(param['default'], dict):
                    param['default'] = json.dumps(param['default'])
                elif not isinstance(param['default'], str):
                    raise ValueError(
                        "The 'external_location' parameter must be a dict or a JSON string."
                    )
            
            # Converting 'schema_permissions' list parameter passed in job_config to desired str
            if param['name'] == 'schema_permissions':
                if isinstance(param['default'], list):
                    param['default'] = json.dumps(param['default'])
                elif not isinstance(param['default'], str):
                    raise ValueError(
                        "The 'schema_permissions' parameter must be a list or a JSON string."
                    )
        
        # Inject default into parameters (Line 216 in job.py.5.jpg)
        params_copy.append({'name': 'ts_dt', 'default': 'CURRENT_DTM'}) 
        
        # Check for required parameters
        has_ext_src_params = all(p in params_names for p in expected_params_ext_src)
        has_hudi_elf_funds_params = all(p in params_names for p in expected_params_hudi_elf_funds)

        if not (has_ext_src_params or has_hudi_elf_funds_params):
            raise ValueError(
                f"Required parameters must include either all of {expected_params_ext_src} or {expected_params_hudi_elf_funds}."
            )

        return params_copy

    # --- build_job() (Starts line 238 in job.py.5.jpg, continues in job.py.6.jpg) ---

    def build_job(self, **kwargs) -> dict:
        """
        Builds a job object from a P1 object
        """
        custom_snippet = kwargs.pop('custom_snippet', self.job_template['custom_snippet']) # Assuming self.job_template is available
        
        job_clusters = self._build_job_clusters(kwargs.pop('job_clusters', self.job_template['job_clusters']), self.job_template['job_clusters_overrides'])
        tasks = self._build_tasks(kwargs.pop('tasks', self.job_template['tasks']), self.job_template['task_overrides'])
        parameters = self._validate_and_build_parameters(kwargs.pop('parameters', self.job_template['parameters']))
        
        if self.job_template['schedule'] and self.job_template['workflow_definition']:
            custom_output = "schedule"
        else:
            custom_output = "job"

        CURRENT_DTM = "dt" # Placeholder
        
        # The snippet below is to inject a new id to objects created with random names when doing test deploys.
        # This prevents collisions during deployment tests.
        if "CURRENT_ENV" in custom_snippet:
            custom_snippet = custom_snippet.replace("CURRENT_ENV", "dev")
        
        service_principal_name = "113de513 81d4 40ba 8ca5 b2c34d0b1239" # Placeholder
        
        custom_job = merge_nested_dict(self.job_template, custom_snippet) # Assuming merge_nested_dict is a utility
        
        print(
            f"DEBUG: Dictionary passed to Job.from_dict():\n{json.dumps(custom_job, indent=2)}"
        )
        
        # Converting the dictionary into a type recognized by job class
        typed_custom_job = Job.from_dict(custom_job) # Assuming Job.from_dict exists
        
        return typed_custom_job

    @classmethod
    def construct_from_job_config(cls, job_config: dict[str, Any]) -> "Job":
        """
        Constructs a MLJob instance from a job configuration dictionary.
        """
        
        env = job_config.pop("ENV", None).lower()

        # create_env_job_config is assumed to be a utility function
        env_job_config = create_env_config(job_config, env) 
        
        name = env_job_config.pop("name")
        parameters = env_job_config.pop("parameters", [])
        job_clusters = env_job_config.pop("job_clusters", [])
        tasks = env_job_config.pop("tasks", [])
        schedule = env_job_config.pop("schedule", {})
        
        return cls({
            "name": name,
            "parameters": parameters,
            "job_clusters": job_clusters,
            "tasks": tasks,
            "schedule": schedule,
        })