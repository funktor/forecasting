from batch import Batch
import datetime
import os
import constants as cnt
import batch_task as bt
from pandas import DataFrame
from blob_storage import Blob
import pandas as pd
import asyncio

async def run(df:DataFrame, 
        blob:Blob, 
        date_partition:str, 
        run_id:str,
        acr_user:str, 
        acr_pwd:str, 
        app_secret:str, 
        app_id:str, 
        tenant_id:str, 
        pool_id:str, 
        batch_url:str, 
        vm_size:str, 
        pool_count:int, 
        image:str, 
        acr_url:str, 
        blob_storage_url:str, 
        max_tasks:int=10,
        task_type:str="train"):
    
    """
    Run batch program.

    :param df: input dataframe
    :param blob: azure blob sdk
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param acr_user: ACR username
    :param acr_pwd: ACR password
    :param app_secret: service principal secret
    :param app_id: service principal AAD app id
    :param tenant_id: tenant id for AAD app
    :param pool_id: batch pool id
    :param vm_size: VM type e.g. "Standard_d4_v3"
    :param pool_count: number of nodes in pool
    :param image: docker image to pull
    :param acr_url: ACR repository url
    :param blob_storage_url: blob storage url
    :param max_tasks: maximum number of tasks to perform
    :param task_type: training or inference
    """
    
    job_id:str = run_id
    
    # Create tasks for batch workers
    all_tasks, async_tasks = bt.create_tasks(\
                                    df, 
                                    blob, 
                                    date_partition, 
                                    run_id, 
                                    max_tasks=max_tasks,
                                    task_type=task_type)
    
    await asyncio.gather(*async_tasks)
    
    # Create batch object
    batch_obj = Batch(batch_url, app_id, tenant_id, app_secret)
    
    try:
        # Create batch pool
        batch_obj.create_pool(pool_id, 
                              vm_size, 
                              pool_count, 
                              image, 
                              acr_url, 
                              acr_user, 
                              acr_pwd)
        
        # Create job
        batch_obj.create_job(job_id, pool_id)
        
        # Add the tasks to the job
        commands, keys = [], []
        
        for new_task in all_tasks:
            keys += [new_task.key]
            
            if task_type == "train":
                path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.TASKS_PATH, 
                                    cnt.TRAINING_TASKS_FOLDER,
                                    new_task.key)
            else:
                path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.TASKS_PATH, 
                                    cnt.PREDICT_TASKS_FOLDER,
                                    new_task.key)
            
            commands += \
                [f"python /src/driver.py \
                    --path \"{path}\" \
                    --app_id {app_id} \
                    --tenant_id {tenant_id} \
                    --app_secret {app_secret} \
                    --blob_storage_url {blob_storage_url}"]
        
        batch_obj.add_tasks(image, "latest", job_id, commands, keys)
        
        # Check if all tasks are completed, timeout is set to 30 minutes
        completed = \
            batch_obj\
            .wait_for_tasks_to_complete(job_id, 
                                        datetime.timedelta(minutes=30))
        
        if completed == 1:
            # If all tasks completed, upload the stdout.txt and 
            # stderr.txt files to blob storage for log analysis
            
            # TODO: Make these uploads asynchronous
            out, err = batch_obj.get_task_outputs_and_errors(job_id)
            async_tasks = []
            
            print("Uploading task outputs...")
            
            for id, output in out:
                if task_type == "train":
                    path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.TASKS_PATH, 
                                    cnt.TRAINING_TASKS_FOLDER,
                                    id)
                else:
                    path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.TASKS_PATH, 
                                    cnt.PREDICT_TASKS_FOLDER,
                                    id)
                
                async_tasks += \
                    [asyncio.create_task(\
                        blob.upload_str_data_to_container_async(\
                            data=output, 
                            output_container=cnt.CONTAINER, 
                            output_file_path=\
                                os.path.join(\
                                    path, 
                                    cnt.TASK_OUTPUT_FILE\
                                )
                        ))]
            
            print("Uploading task errors...")
            
            for id, error in err:
                if task_type == "train":
                    path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.TASKS_PATH, 
                                    cnt.TRAINING_TASKS_FOLDER,
                                    id)
                else:
                    path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.TASKS_PATH, 
                                    cnt.PREDICT_TASKS_FOLDER,
                                    id)
                
                async_tasks += \
                    [asyncio.create_task(\
                        blob.upload_str_data_to_container_async(\
                            data=error, 
                            output_container=cnt.CONTAINER, 
                            output_file_path=\
                                os.path.join(\
                                    path, 
                                    cnt.TASK_ERRORS_FILE\
                                )
                        ))]
            
            await asyncio.gather(*async_tasks)
            
            if task_type == "predict": 
                predict_tasks = []
                path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.OUTPUTS_PATH)
                
                file_paths = []
                
                for key in keys:
                    file_path = os.path.join(path, f"{key}.csv")
                    file_paths += [file_path]
                    
                    predict_tasks += \
                        [asyncio.create_task(\
                            blob.write_blob_to_local_file_async(\
                                cnt.CONTAINER, 
                                file_path))]
                
                await asyncio.gather(*predict_tasks)
                
                dfs = []
                
                for file_path in file_paths:
                    df_file = pd.read_csv(\
                                file_path, 
                                sep=",", 
                                index_col=None, 
                                header=0)
                    
                    df_file = \
                        df_file.set_index(\
                            keys=[cnt.FORECAST_TIMESTAMP_COLUMN, 
                                  cnt.PRODUCT_COLUMN, 
                                  cnt.REGION_COLUMN], 
                            drop=False)
                        
                    dfs.append(df_file)

                final_df = pd.concat(dfs, 
                                     axis=0, 
                                     ignore_index=True)\
                                        .reset_index(drop=True)
                
                # Upload new dataframe to blob storage   
                path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.FINAL_PREDICTIONS_FILE)
                
                await \
                    blob.upload_pandas_dataframe_to_container_async(\
                                final_df, 
                                cnt.CONTAINER, 
                                path)
                
                print("Done!!!")
            
        else:
            raise Exception("Timeout happened for task completion")
            
    except Exception as e:
        print(e)
    finally:
        # Delete pool and job on completion or exception
        batch_obj.delete_job(job_id)
        batch_obj.delete_pool(pool_id)
        pass