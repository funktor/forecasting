import joblib
import constants as cnt
from blob_storage import Blob
import os
from batch_task import Task
import helper

def run(\
        path:str, 
        app_id:str, 
        tenant_id:str, 
        app_secret:str, 
        blob_storage_url:str):
    """
    Run task specified by the input file at path
    
    :param path: Task input file path w/o file name
    :param app_id: Client app id
    :param tenant_id: Client tenant id
    :param app_secret: Client app secret that has access to storage account
    :param blob_storage_url: Blob storage URL
    :param blob_storage_inp_container: Blob storage container for storing task inputs
    :param blob_storage_op_container: Blob storage container for storing task outputs
    """
    blob = Blob(blob_storage_url, app_id, tenant_id, app_secret)
    
    print("Reading tasks into local...")
    file_path = os.path.join(path, cnt.TASKS_FILE)
    new_task:Task = helper.deserialize_file(file_path, blob)
    
    print("Running tasks...")
    new_task.run(blob)
    
    blob.blob_service_client.close()
    blob.blob_service_client_async.close()