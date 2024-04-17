import os
import helper
from pandas import DataFrame
import model as md
import constants as cnt
from blob_storage import Blob
import pandas as pd
import datetime

def predict(key:str, 
            blob:Blob, 
            date_partition:str, 
            run_id:str, 
            num_preds:int=60):
    """
    Time series model forecasting.

    :param key: key (product__region) for plots
    :param blob: azure blob sdk
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param num_preds: number of steps in future to predict
    """
    
    # Do predictions
    # TODO: Build prediction API
    # TODO: Improve performance of this code
    outputs = md.forecast(key, 
                          blob, 
                          date_partition, 
                          run_id,
                          cnt.FULL_MODEL, 
                          cnt.MODEL_SEED_FILE_NAME,
                          num_preds)
    
    # Corresponding date for each time step in future
    dates = []
    curr_date = datetime.datetime.strptime(date_partition, "%Y/%m/%d")
    
    for _ in range(num_preds):
        curr_date += datetime.timedelta(days=1)
        curr_date_str = datetime.datetime.strftime(curr_date, "%Y-%m-%d")
        dates += [curr_date_str]
        
    print(dates)

    # Read the dataframe corresponding to the key
    path = os.path.join(cnt.FORECAST_ROOT, 
                        date_partition, 
                        run_id, 
                        cnt.DATA_PATH, 
                        f"{key}.pkl")
    
    df_one:DataFrame = helper.deserialize_file(path, blob)
        
    if df_one is not None:
        product = list(df_one[cnt.PRODUCT_COLUMN])[0]
        region = list(df_one[cnt.REGION_COLUMN])[0]
        
        # Create new dataframe with forecast date and forecast cores
        new_df = \
            pd.DataFrame({\
                cnt.FORECAST_TIMESTAMP_COLUMN:dates,
                cnt.PRODUCT_COLUMN:[product]*len(outputs), 
                cnt.REGION_COLUMN:[region]*len(outputs), 
                cnt.FORECAST_COLUMN:outputs\
            })
        
        # Upload new dataframe to blob storage   
        path = os.path.join(cnt.FORECAST_ROOT, 
                        date_partition, 
                        run_id, 
                        cnt.OUTPUTS_PATH, 
                        f"{key}.csv")
        
        blob\
            .upload_pandas_dataframe_to_container(\
                new_df, 
                cnt.CONTAINER, 
                path)
        