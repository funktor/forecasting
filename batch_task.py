import train, helper, predict
from pandas import DataFrame
import constants as cnt
import os
from blob_storage import Blob
import random
import asyncio

class Task:
    # Parent Task class
    def __init__(self, key, date_partition, run_id):
        self.key = key
        self.date_partition = date_partition
        self.run_id = run_id
    
    def run(self, blob:Blob):
        pass
            
class TrainTask(Task):
    # Training task class
    def run(self, blob:Blob):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(\
            train.train(\
                    self.key, 
                    blob, 
                    self.date_partition, 
                    self.run_id))
        
class PredictTask(Task):
    # Prediction task class
    def run(self, blob:Blob):
        predict.predict(self.key, 
                        blob, 
                        self.date_partition, 
                        self.run_id)
        
def do_process(df:DataFrame, 
               combined:list,
               blob:Blob, 
               date_partition:str, 
               run_id:str, 
               product:str, 
               region:str,
               async_tasks:list, 
               min_df_size:int=100):
    """
    Filter data on product and region, create partitioned dataframes and upload to blob storage

    :param df: pandas dataframe of input data
    :param blob: blob storage object
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param product: product to filter
    :param region: region to filter
    :param min_df_size: minimum size of filtered dataframe to be considered for forecasting
    """
    # TODO: Can the key contain additional characters that can cause issues?
    key = product.replace(" ", "_") + "--" + region.replace(" ", "_")
    print(f"Key={key}")

    folder_path = \
        os.path.join(cnt.FORECAST_ROOT, 
                     date_partition, 
                     run_id, 
                     cnt.DATA_PATH)
    
    # Filter based on Product and Region
    print("Filtering based on key...")
    
    # Do binary search to find start and end indices for
    # current product and region.
    # TODO: Implement binary search using sortedcontainers
    s = helper.get_index_in_sorted_array(\
                    combined, 
                    (product, region), 
                    begin=True)
    
    e = helper.get_index_in_sorted_array(\
                    combined, 
                    (product, region), 
                    begin=False)
    
    # Filter start to end rows from dataframe
    
    # We are using sorting + binary search because filtering
    # with original df is linear and time complexity is O(M*N)
    # where M is number of searches and N is number of rows.
    # Performance did not improve much on using the searchable columns as index
    # Sorting + binary search has complexity of O(NlogN + MlogN)
    
    df_one:DataFrame = \
        df.iloc[s:(e+1)].reset_index(drop=True)

    # Upload filtered dataframe to blob storage
    # TODO: Handle cases where some uploads fails
    # TODO: These dataframes are not required by pipeline but useful for debugging. 
    if (len(df_one) - cnt.IN_STEPS) > min_df_size:
        async_tasks += [asyncio.create_task(\
                            helper.serialize_object_async(\
                                df_one, 
                                blob, 
                                folder_path, 
                                f"{key}.pkl"))]
        
        return key, async_tasks

    return None, async_tasks

def create_tasks(\
                df:DataFrame, 
                blob:Blob, 
                date_partition:str, 
                run_id:str, 
                max_tasks:int=10, 
                task_type:str="train")->list[Task]: 
    """
    Create tasks for batch and upload task files to blob storage.

    :param df: pandas dataframe of input data
    :param blob: blob storage object
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param max_tasks: maximum number of tasks to perform
    :param task_type: training or inference
    :return: list of tasks
    """
    async_tasks = []
    all_tasks = []
    
    if task_type == "train":
        # Filter NaN in product and region columns
        df = df[(df[cnt.PRODUCT_COLUMN].notna())&(df[cnt.REGION_COLUMN].notna())]
        
        # Partition the data on product and region
        df_keys = \
            df[[cnt.PRODUCT_COLUMN, cnt.REGION_COLUMN]]\
            .drop_duplicates()\
            .reset_index()
        
        # Sort dataframe on product and region
        df = df.sort_values(\
                by=[cnt.PRODUCT_COLUMN, cnt.REGION_COLUMN])\
                    .reset_index(drop=True)
            
        prods = list(df[cnt.PRODUCT_COLUMN])
        regns = list(df[cnt.REGION_COLUMN])
        
        combined = list(zip(prods, regns))
            
        keys = []
        max_tasks = len(df_keys) if max_tasks < 0 else max_tasks
        
        for i in random.sample(list(range(len(df_keys))), min(len(df_keys), max_tasks)):
            _, product, region = list(df_keys.loc[i])
            
            print(f"Processing product={product} and region={region}")
            res, async_tasks = \
                do_process(\
                    df, 
                    combined,
                    blob, 
                    date_partition, 
                    run_id, 
                    product, 
                    region, 
                    async_tasks)
                
            if res is not None:
                keys += [res]
        
        # Save the keys for prediction step
        keys_folder_path = \
            os.path.join(cnt.FORECAST_ROOT, 
                            date_partition, 
                            run_id, 
                            cnt.DATA_PATH)
        
        async_tasks += [asyncio.create_task(\
                            helper.serialize_object_async(\
                                keys, 
                                blob, 
                                keys_folder_path, 
                                cnt.KEYS_FILE))] 
        
        # Upload tasks to blob storage
        # TODO: Handle cases where some uploads fails
        # TODO: Can we improve performance by collating all input tasks into a single file and upload single file. 
        # During prediction, only a single disk seek is required and single network I/O.
        for key in keys:
            new_task = TrainTask(key, date_partition, run_id)
            
            path = \
                os.path.join(cnt.FORECAST_ROOT, 
                            date_partition, 
                            run_id, 
                            cnt.TASKS_PATH, 
                            cnt.TRAINING_TASKS_FOLDER,
                            key)
            
            async_tasks += [asyncio.create_task(\
                                helper.serialize_object_async(\
                                    new_task, 
                                    blob, 
                                    path, 
                                    cnt.TASKS_FILE))]   
            all_tasks += [new_task]
            
    else:
        path = os.path.join(cnt.FORECAST_ROOT, 
                            date_partition, 
                            run_id, 
                            cnt.DATA_PATH, 
                            cnt.KEYS_FILE)
        
        # Retrieve the keys stored during training step
        keys = helper.deserialize_file(path, blob)
        
        for key in keys:
            new_task = PredictTask(key, date_partition, run_id)
            
            folder_path = \
                os.path.join(cnt.FORECAST_ROOT, 
                            date_partition, 
                            run_id, 
                            cnt.TASKS_PATH, 
                            cnt.PREDICT_TASKS_FOLDER,
                            key)
            
            async_tasks += [asyncio.create_task(\
                                helper.serialize_object_async(\
                                    new_task, 
                                    blob, 
                                    folder_path, 
                                    cnt.TASKS_FILE))]      
            all_tasks += [new_task]
    
    return all_tasks, async_tasks