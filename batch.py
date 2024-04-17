import datetime
import io
import sys
import time

from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels
import constants
from azure.common.credentials import ServicePrincipalCredentials

def _read_stream_as_string(stream, encoding="utf-8"):
    """
    Read stream as string

    :param stream: input stream generator
    :param encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
            
        return output.getvalue().decode(encoding)
    finally:
        output.close()
        
def get_container_registry(acr_url:str=None, 
                           acr_username:str=None, 
                           acr_password:str=None):
    """
    Get container registry for batch
    
    :param acr_username: ACR account username
    :param acr_password: ACR account password
    :returns: Container registry object
    """
    
    container_registry = \
        batchmodels.ContainerRegistry(
            registry_server=acr_url,
            user_name=acr_username,
            password=acr_password
        )
        
    return container_registry

class Batch:
    def __init__(self, 
                 batch_account_url:str, 
                 app_id:str, 
                 tenant_id:str, 
                 secret:str):
        """
        Batch initialize
        
        :param batch_account_url: batch account url
        :param app_id: client app id
        :param tenant_id: client tenant id
        :param secret: app client secret
        """
            
        credentials = \
            ServicePrincipalCredentials(\
                client_id=app_id,
                secret=secret,
                tenant=tenant_id,
                resource="https://batch.core.windows.net/"
            )
            
        self.batch_service_client = \
            BatchServiceClient(\
                credentials=credentials, 
                batch_url=f"https://{batch_account_url}"
            )
        
    def create_pool(self, 
                    pool_id:str, 
                    vm_size:str,
                    pool_node_count:int,
                    image_name:str, 
                    acr_url:str,
                    acr_username:str, 
                    acr_password:str):
        """
        Creates a pool of compute nodes with the specified OS settings.

        :param pool_id: An ID for the new pool.
        :param vm_size: VM Type for pool.
        :param pool_node_count: Number of nodes in pool.
        :param image_name: ACR image name (format: ACR repo/image_name)
        :param acr_url: ACR account url
        :param acr_username: ACR account username
        :param acr_password: ACR account password
        """
        print(f'Creating pool [{pool_id}]...')
        
        container_registry = \
            get_container_registry(acr_url, 
                                   acr_username, 
                                   acr_password)
        
        container_conf = \
            batchmodels.ContainerConfiguration(
                container_image_names=[image_name],
                container_registries=[container_registry]
            )

        image_ref_to_use = \
            batchmodels.ImageReference(
                publisher='microsoft-azure-batch',
                offer='ubuntu-server-container',
                sku="20-04-lts",
                version="latest"
            )
        
        new_pool = batchmodels.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=\
                batchmodels.VirtualMachineConfiguration(
                    image_reference=image_ref_to_use,
                    container_configuration=container_conf,
                    node_agent_sku_id="batch.node.ubuntu 20.04"
                ),
            vm_size=vm_size,
            target_dedicated_nodes=pool_node_count
        )
        
        # Create pool if not exists
        if not self.batch_service_client.pool.exists(pool_id):
            self.batch_service_client.pool.add(new_pool)


    def create_job(self, 
                   job_id:str, 
                   pool_id:str):
        """
        Creates a job with the specified ID, associated with 
        the specified pool.

        :param job_id: The ID for the job.
        :param pool_id: The ID for the pool.
        """
        print(f'Creating job [{job_id}]...')

        job = \
            batchmodels.JobAddParameter(
                id=job_id,
                pool_info=batchmodels.PoolInformation(pool_id=pool_id)
            )

        self.batch_service_client.job.add(job)


    def add_tasks(self, 
                  image_name:str, 
                  image_version:str, 
                  job_id:str, 
                  commands:list, 
                  tags:list):
        """
        Adds a task for each command in the collection to the 
        specified job.

        :param image_name: ACR image name
        :param image_version: ACR image version
        :param job_id: The ID of the job to which to add the tasks.
        :param commands: Commands to run when container starts.
        :param tags: Tags for each task.
        """

        print(f'Adding tasks to job [{job_id}]...')

        user = \
            batchmodels.UserIdentity(
                auto_user=\
                    batchmodels.AutoUserSpecification(
                        elevation_level=batchmodels.ElevationLevel.admin,
                        scope=batchmodels.AutoUserScope.task
                    )
            )

        # Delete container when it ends, also change working
        # directory to /src after container starts
        task_container_settings = \
            batchmodels.TaskContainerSettings(
                image_name=image_name + ':' + image_version,
                container_run_options='--rm --workdir /src'
            )
        
        i = 0
        tasks = []
        
        while i < len(commands):
            task = batchmodels.TaskAddParameter(
                id=tags[i],
                command_line=commands[i],
                container_settings=task_container_settings,
                user_identity=user
            )
            
            i += 1
            tasks.append(task)
            
        self.batch_service_client.task.add_collection(job_id, tasks)


    def wait_for_tasks_to_complete(self, 
                                   job_id: str, 
                                   timeout: datetime.timedelta):
        """
        Returns when all tasks in the specified job reach the 
        Completed state or Timeout happens

        :param job_id: The id of the job whose tasks should be to monitored.
        :param timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period.
        :returns: -1 if timeout occurs else 1
        """
        timeout_expiration = datetime.datetime.now() + timeout

        print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

        while datetime.datetime.now() < timeout_expiration:
            print('.', end='')
            sys.stdout.flush()
            tasks = self.batch_service_client.task.list(job_id)
            incomplete_tasks = [task for task in tasks if task.state != batchmodels.TaskState.completed]
            
            if len(incomplete_tasks) == 0:
                return 1

            time.sleep(1)

        return -1


    def get_task_outputs_and_errors(self, 
                                    job_id: str, 
                                    text_encoding:str="utf-8"):
        """
        Prints the output file for each task in the job.

        :param job_id: The id of the job with task output files to print.
        :param text_encoding: The encoding of the output file. The default is utf-8.
        """

        print('Printing task output...')

        tasks = self.batch_service_client.task.list(job_id)
        outputs, errors = [], []
        
        for task in tasks:
            node_id = \
                self.batch_service_client\
                    .task.get(job_id, task.id).node_info.node_id

            out_stream = \
                self.batch_service_client.file\
                    .get_from_task(\
                        job_id, 
                        task.id, 
                        constants.TASK_OUTPUT_FILE\
                    )
                    
            err_stream = \
                self.batch_service_client.file\
                    .get_from_task(\
                        job_id, 
                        task.id, 
                        constants.TASK_ERRORS_FILE\
                    )

            out_text = _read_stream_as_string(out_stream, text_encoding)
            err_text = _read_stream_as_string(err_stream, text_encoding)
            
            outputs += [(task.id, out_text)]
            errors += [(task.id, err_text)]
            
        return outputs, errors
            
    def delete_pool(self, pool_id):
        """
        Delete pool

        :param pool_id: The id of the pool to be deleted
        """
        
        print(f'Deleting pool id [{pool_id}]...')
        if self.batch_service_client.pool.exists(pool_id):
            self.batch_service_client.pool.delete(pool_id)
        
    def delete_job(self, job_id):
        """
        Delete job

        :param job_id: The id of the job to be deleted
        """
        
        print(f'Deleting job id [{job_id}]...')
        self.batch_service_client.job.delete(job_id)
    
    def delete_task(self, task_id, job_id):
        """
        Delete task

        :param task_id: The id of the task to be deleted
        :param job_id: The id of the job to be deleted
        """
        
        print(f'Deleting task id [{task_id}]...')
        self.batch_service_client.task.delete(job_id, task_id)
        
    def create_job_schedule(self, 
                            job_schedule_id, 
                            s_vm_size,
                            s_node_pool_count,
                            s_interval,
                            image_name, 
                            image_version,
                            acr_url,
                            acr_username, 
                            acr_password, 
                            command):
        """Creates an Azure Batch pool and job schedule with the specified ids.

        :param job_schedule_id: ID for job schedules
        :param s_vm_size: Scheduler VM Type
        :param s_node_pool_count: Scheduler pool size
        :param s_interval: Interval for recurring job
        :param image_name: ACR image name (format: ACR repo/image_name)
        :param image_version: ACR image version
        :param acr_url: ACR url
        :param acr_username: ACR account username
        :param acr_password: ACR account password
        :param command: command to run
        """
        
        print(f'Creating job schedule [{job_schedule_id}]...')
        
        container_conf = None
        
        # Check if is using containers
        if image_name is not None:
            container_registry = \
                get_container_registry(acr_url, 
                                       acr_username, 
                                       acr_password)
            
            container_conf = \
                batchmodels.ContainerConfiguration(
                    container_image_names=[image_name],
                    container_registries=[container_registry]
                )

        image_ref_to_use = \
            batchmodels.ImageReference(
                publisher='microsoft-azure-batch',
                offer='ubuntu-server-container',
                sku="20-04-lts",
                version="latest"
            )
            
        vm_configuration = \
            batchmodels.VirtualMachineConfiguration(
                image_reference=image_ref_to_use,
                container_configuration=container_conf,
                node_agent_sku_id="batch.node.ubuntu 20.04"
            )
            
        pool_specification = \
            batchmodels.PoolSpecification(
                vm_size=s_vm_size,
                target_dedicated_nodes=s_node_pool_count,
                virtual_machine_configuration=vm_configuration
            )

        pool_info = \
            batchmodels.PoolInformation(
                auto_pool_specification=\
                    batchmodels.AutoPoolSpecification(
                        auto_pool_id_prefix="JobScheduler",
                        pool=pool_specification,
                        keep_alive=False,
                        pool_lifetime_option=\
                            batchmodels.PoolLifetimeOption.job
                    )
            )
            
        user = \
            batchmodels.UserIdentity(
                auto_user=\
                    batchmodels.AutoUserSpecification(
                        elevation_level=batchmodels.ElevationLevel.admin,
                        scope=batchmodels.AutoUserScope.task
                    )
            )

        # Delete container when it ends, also change working
        # directory to /src after container starts
        task_container_settings = \
            batchmodels.TaskContainerSettings(
                image_name=image_name + ':' + image_version,
                container_run_options='--rm --workdir /src'
            )

        # Job spec
        job_spec = \
            batchmodels.JobSpecification(
                pool_info=pool_info,
                on_all_tasks_complete=\
                    batchmodels.OnAllTasksComplete.no_action, # Delete job explicitly
                job_manager_task=\
                    batchmodels.JobManagerTask(
                        id="JobManagerTask",
                        container_settings=task_container_settings,
                        user_identity=user,
                        command_line=command
                    )
            )

        # Specify the interval of the job scheduler
        schedule = \
            batchmodels.Schedule(
                recurrence_interval=\
                    datetime.timedelta(\
                        seconds=s_interval)
            )

        # Create job schedule
        scheduled_job = \
            batchmodels.JobScheduleAddParameter(
                id=job_schedule_id,
                schedule=schedule,
                job_specification=job_spec
            )

        # Add job schedule to batch_service_client
        self.batch_service_client\
            .job_schedule.add(cloud_job_schedule=scheduled_job)
    
    def delete_job_schedule(self, job_schedule_id):
        """
        Delete job

        :param job_schedule_id: The id of the scheduled job to be deleted
        """
        
        print(f'Deleting job schedule id [{job_schedule_id}]...')
        self.batch_service_client\
            .job_schedule.delete(job_schedule_id)
        