import tensorflow as tf
from tensorflow import keras
from keras.layers import Input, Conv1D, Dense, Reshape, LSTM
from keras.models import Model, load_model
import os
from pandas import DataFrame, Series
import helper
import numpy as np
import joblib
import constants as cnt
from blob_storage import Blob

# TODO: Add bayesian hyperparameter tuning
# TODO: Add model checkpoints
# TODO: Add validation data
# TODO: Try out different models LSTM, Transformers etc.

def model_ts(inp_shape:tuple, out_shape:tuple)->Model:
    """
    Initialize Tensorflow Model instance.

    :param inp_shape: shape of the input (batch, STEPS, 1)
    :param out_shape: shape of the output (batch, STEPS, 1)
    :return: Tensorflow Model object
    """
    inp = Input(shape=inp_shape)
    # (batch, IN_STEPS, 1)
    x = Conv1D(filters=4096, 
               kernel_size=inp_shape[0], 
               activation='relu', 
               input_shape=inp_shape)(inp)
    # x = LSTM(64, activation='relu', input_shape=inp_shape)(inp)
    # (batch, 1, 32768)
    x = Dense(out_shape[0], 
              kernel_initializer=tf.initializers.zeros())(x)
    # (batch, 1, OUT_STEPS)
    out = Reshape(out_shape)(x)
    # (batch, OUT_STEPS, 1)

    model = Model(inp, out)
    
    model.compile(loss='mean_squared_error', 
                  optimizer=tf.keras.optimizers.Adam(0.001))

    return model

def run_model(model:Model, 
              X_train:np.ndarray, 
              y_train:np.ndarray, 
              epochs:int=100, 
              batch_size:int=32):
    """
    Train a Tensorflow model.

    :param model: Tensorflow Model object
    :param X_train: training data
    :param y_train: training labels
    :param epochs: number of epochs to train
    :param batch_size: batch size in each epoch
    """
    
    model.fit\
    (
        X_train, 
        y_train, 
        epochs=epochs, 
        batch_size=batch_size, 
        validation_split=None, 
        verbose=1, 
        shuffle=False
    )
    
    return model

def forecast(key:str, 
             blob:Blob, 
             date_partition:str, 
             run_id:str,
             model_tag:str, 
             seed_tag:str,
             num_steps:int)->list:
    """
    Return forecast from model.

    :param key: key (product__region) for plots
    :param blob: azure blob sdk
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param model_tag: tag of model provided by user
    :param seed_tag: tag of seed data for model provided by user
    :param num_steps: number of steps into the future to forecast
    :return: forecasted values
    """
    
    model_path = os.path.join(cnt.FORECAST_ROOT, 
                              date_partition, 
                              run_id, 
                              cnt.MODEL_PATH, 
                              key, 
                              model_tag)
    
    seed_data_path = \
        os.path.join(cnt.FORECAST_ROOT, 
                     date_partition, 
                     run_id, 
                     cnt.DATA_PATH, 
                     key, 
                     seed_tag)
    
    outputs = []
    
    try:
        # Download model from blob storage to local and load in memory
        blob.write_blob_to_local_file(cnt.CONTAINER, model_path)
        model:Model = load_model(model_path)
        
        # Download seed data from blob storage to local and load in memory
        blob.write_blob_to_local_file(cnt.CONTAINER, seed_data_path)
        
        seed_data, _std, _mean = \
            joblib.load(seed_data_path)
            
        curr_inp = seed_data
        
        # TODO: Need to improve the performance of this loop
        for _ in range(num_steps):
            preds:np.ndarray = \
                model\
                    .predict(curr_inp\
                        .reshape((1, curr_inp.shape[0], curr_inp.shape[1])))
            w = preds[0]*_std+_mean
            outputs += [w[0][0]]
            curr_inp = np.concatenate([curr_inp[1:], preds[0]])
            
    except Exception as e:
        print(e)

    return outputs