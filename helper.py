import matplotlib.pyplot as plt
import os
import joblib
import constants as cnt
from tensorflow import keras
from keras.models import Model
from blob_storage import Blob
import numpy as np
import math
        
def serialize_object(object, 
                     blob:Blob, 
                     folder_path:str, 
                     file_name:str)->None:
    """
    Serialize object onto disk.

    :param object: object to serialize
    :param blob: azure blob sdk
    :param folder_path: folder location on disk
    :param file_name: file name for storage
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    path:str = os.path.join(folder_path, file_name)
    
    joblib.dump(object, path)
    blob.upload_file_to_container(path, cnt.CONTAINER, path)
    
async def serialize_object_async(object, 
                     blob:Blob, 
                     folder_path:str, 
                     file_name:str)->None:
    """
    Serialize object onto disk (asynchronous).

    :param object: object to serialize
    :param blob: azure blob sdk
    :param folder_path: folder location on disk
    :param file_name: file name for storage
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    path:str = os.path.join(folder_path, file_name)
    
    joblib.dump(object, path)
    await blob.upload_file_to_container_async(path, 
                                              cnt.CONTAINER, 
                                              path)
    
def deserialize_file(file_path:str, 
                     blob:Blob)->None:
    """
    Deserialize file from disk.

    :param file_path: file to deserialize
    :param blob: azure blob sdk
    """
    object = None
    
    try:
        blob.write_blob_to_local_file(cnt.CONTAINER, file_path)
        object = joblib.load(file_path)
    except Exception as e:
        print(e)
        
    return object

async def deserialize_file_async(file_path:str, blob:Blob)->None:
    """
    Deserialize file from disk (asynchronous).

    :param file_path: file to deserialize
    :param blob: azure blob sdk
    """
    object = None
    
    try:
        await \
            blob.write_blob_to_local_file_async(cnt.CONTAINER, 
                                                file_path)
        object = joblib.load(file_path)
    except Exception as e:
        print(e)
        
    return object
    
def serialize_model_object(model:Model, 
                     blob:Blob, 
                     folder_path:str, 
                     file_name:str)->None:
    """
    Serialize object onto disk.

    :param model: Model object to serialize
    :param blob: azure blob sdk
    :param folder_path: folder location on disk
    :param file_name: file name for storage
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    path:str = os.path.join(folder_path, file_name)
    
    model.save(path)
    blob.upload_file_to_container(path, cnt.CONTAINER, path)
    
async def serialize_model_object_async(\
                model:Model, 
                blob:Blob, 
                folder_path:str, 
                file_name:str)->None:
    """
    Serialize object onto disk (asynchronous).

    :param model: Model object to serialize
    :param blob: azure blob sdk
    :param folder_path: folder location on disk
    :param file_name: file name for storage
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    path:str = os.path.join(folder_path, file_name)
    
    # TODO: Implement asynchronous model save api in Tensorflow
    model.save(path)
    await blob.upload_file_to_container_async(path, 
                                              cnt.CONTAINER, 
                                              path)

def plot_figure(key:str, 
                blob:Blob, 
                date_partition:str, 
                run_id:str,
                image_title:str, 
                values:list[list], 
                labels:list, 
                titles:list, 
                use_subplots=True)->None:
    """
    Plot graphs using pyplot library.

    :param key: key (product__region) for plots
    :param blob: azure blob sdk
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param image_title: title for the image
    :param values: values to plot in the y-axis. Its a list of list for multiple plots
    :param labels: labels for y-axis
    :param titles: title for each sub-plot if use_subplots=True
    :param use_subplots: use sub plots in a single image
    """
    
    plt.figure(figsize=(12,5), dpi=100)

    if use_subplots and len(values) > 1:
        fig, axes = plt.subplots(len(values), 1, sharex=True)
    
        for i in range(len(values)):
            axes[i].plot(values[i], label=labels[i])
            axes[i].set_title(titles[i])
            axes[i].legend(loc='upper left', fontsize=8)
    else:
        for i in range(len(values)):
            plt.plot(values[i], label=labels[i])
            
        plt.title(titles[0])
        plt.legend(loc='upper left', fontsize=8)

    folder_path = os.path.join(cnt.FORECAST_ROOT, 
                               date_partition, 
                               run_id, 
                               cnt.PLOTS_PATH, 
                               key)
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    path = os.path.join(folder_path, f"{image_title}.png")
    plt.savefig(os.path.join(folder_path, f"{image_title}.png"))
    blob.upload_file_to_container(path, cnt.CONTAINER, path)
    
    plt.show()
    
def get_index_in_sorted_array(arr, x, begin:bool=True):
    """
    Get index of 1st or last index of x in sorted array arr.

    :param arr: sorted input array
    :param x: element to search
    :param begin: True if returning 1st index else False
    :return: 1st or last index of x in arr
    """
    left, right = 0, len(arr)-1
    index = -1
    
    while left <= right:
        mid = int((left + right)/2)
        if begin:
            if arr[mid] >= x:
                index = mid
                right = mid-1
            else:
                left = mid+1
        else:
            if arr[mid] <= x:
                index = mid
                left = mid+1
            else:
                right = mid-1
                
    return index