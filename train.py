import numpy as np
import os
import joblib
import helper
from pandas import DataFrame, Series
import model as md
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.ensemble import IsolationForest
from tensorflow import keras
from keras.models import Model
import constants as cnt
from blob_storage import Blob
import asyncio

def create_dataset(data:Series, 
                   input_steps:int=1, 
                   output_steps:int=1)->[np.ndarray, np.ndarray]:
    """
    Return forecast from model.

    :param data: pandas dataframe of inputs
    :param input_steps: number of historical steps for time series
    :param output_steps: number of steps into future to predict
    :return: training data and labels
    """
    
    Xs, Ys = [], []
    
    for i in range(len(data) - input_steps - output_steps):
        u = data.iloc[i:(i + input_steps)].values
        v = data.iloc[(i + input_steps):(i + input_steps + output_steps)].values
        Xs.append(u)        
        Ys.append(v)

    Xs = np.array(Xs)
    Xs = Xs.reshape(Xs.shape[0], Xs.shape[1], 1)

    Ys = np.array(Ys)
    Ys = Ys.reshape(Ys.shape[0], Ys.shape[1], 1)
    
    return Xs, Ys
    
async def train(\
            key:str, 
            blob:Blob, 
            date_partition:str, 
            run_id:str)->None:
    """
    Train time series forecasting.

    :param key: key (product__region) for plots
    :param blob: azure blob sdk
    :param date_partition: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    :param run_id: partitioning in blob storage on date and run id e.g. 2023/10/27/<run_id>/
    """
    print(f"Training on key={key}...")
    
    path = os.path.join(cnt.FORECAST_ROOT, 
                        date_partition, 
                        run_id, 
                        cnt.DATA_PATH, 
                        f"{key}.pkl")
    
    # Download filtered dataframe and write to local
    # TODO: Use concurrent downloading and training of models
    # TODO: what if download fails, use retry?
    df_one:DataFrame = helper.deserialize_file(path, blob)

    if df_one is not None:
        # Select Timestamp and PhysicalCores
        df_filt:DataFrame = df_one[[cnt.TIMESTAMP_COLUMN, cnt.NUM_CORES_COLUMN]].copy()
        df_filt.rename(columns={cnt.NUM_CORES_COLUMN:"cores"}, inplace=True)
    
        # Replace NULL with 0 values for PhysicalCores
        df_filt.fillna(0, inplace=True)
    
        helper.plot_figure(key, 
                           blob, 
                           date_partition, 
                           run_id,
                           "orig_physical_cores", 
                           [df_filt.cores], 
                           ["num_cores"], 
                           ["Physical Cores"])
    
        # Do FFT transform of the values to get the period of the series
        print("Doing FFT Transform for period calculation...")
        fft = np.fft.rfft(df_filt.cores)
        fft = abs(fft)
    
        helper.plot_figure(key, 
                           blob, 
                           date_partition, 
                           run_id,
                           "fft_transform", 
                           [fft], 
                           ["frequency"], 
                           ["FFT Transform"])
    
        # Find period of series from FFT transformation
        # period of the data is around 7 days, the values have a weekly cycle
        
        frequencies = np.argsort(fft)[::-1][:20]
        print("Top 20 frequencies and periods...")
        print(list(zip(frequencies, [len(df_filt)/j if j != 0 else -1 for j in frequencies])))
    
        # Decompose time series into trend, seasonal and residual components
        print("Doing time series decomposition...")
        result = seasonal_decompose(df_filt.cores, 
                                    model='additive', 
                                    extrapolate_trend='freq', 
                                    period=7*cnt.GRANULARITY)
        print(result.trend)
        print(result.seasonal)
        print(result.resid)
        print(result.observed)
    
        # Plot decomposed time series
    
        helper.plot_figure(\
            key, 
            blob, 
            date_partition, 
            run_id,
            "series_decomposition", 
            [result.trend, result.seasonal, result.resid], 
            ["trend", "seasonality", "residual"], 
            ["Trend", "Seasonality", "Residual"]\
        )
    
        # Perform anomaly detection on residual series.
        # Here we are using the Isolation Forest algorithm
    
        print("Doing anomaly detection on residuals...")
        
        anomaly_data = [[x] for x in result.resid]
        clf = IsolationForest(random_state=0).fit(anomaly_data)
        out = clf.predict(anomaly_data)
    
        # Replace anomaly values with moving average of input timesteps
        # TODO: Plot anomalies on original plot
        
        print("Replacing anomalies with last 24 hrs MA...")
        
        # anomaly_indices = [i for i in range(len(out)) if out[i] == -1]
        # anomaly_indices_prev = [[max(0,i-cnt.IN_STEPS), max(0,i-1)] for i in anomaly_indices]
        # prev_averages = [df_filt["cores"].loc[x:y].mean() for x, y in anomaly_indices_prev]
    
        # df_filt.loc[anomaly_indices, "cores"] = prev_averages
    
        helper.plot_figure(key, 
                           blob, 
                           date_partition, 
                           run_id,
                           "anomaly_adjusted_physical_cores", 
                           [df_filt.cores], 
                           ["num_cores"], 
                           ["Physical Cores"])

        # Create Training and Test
        # 90% data is used for training and 10% for testing
        print("Creating train test data...")
        
        n = int(0.9*len(df_filt))
        df_train, df_test = df_filt.cores[:n], df_filt.cores[n:]
    
        _mean, _std = df_train.mean(), df_train.std()
        df_train = (df_train - _mean) / _std
    
        X_train, y_train = \
            create_dataset(df_train, cnt.IN_STEPS, cnt.OUT_STEPS)
        
        print("Saving train test data...")
        folder_path = os.path.join(cnt.FORECAST_ROOT, 
                                   date_partition, 
                                   run_id, 
                                   cnt.DATA_PATH, 
                                   key)
        
        helper.serialize_object([X_train, y_train], 
                                blob, 
                                folder_path, 
                                cnt.VALIDATION_DATA)
        
        print("Saving validation seed data...")
        helper.serialize_object((X_train[-1], _std, _mean), 
                                blob, 
                                folder_path, 
                                cnt.VALIDATION_SEED_DATA)
    
        print("Training validation model...")
        model:Model = md.model_ts(X_train[0].shape, y_train[0].shape)
        
        md.run_model(model, 
                     X_train, 
                     y_train, 
                     epochs=100, 
                     batch_size=1024)
        
        print("Saving validation model...")
        folder_path = os.path.join(cnt.FORECAST_ROOT, 
                                   date_partition, 
                                   run_id, 
                                   cnt.MODEL_PATH, 
                                   key)
        
        helper.serialize_model_object(model, 
                                      blob, 
                                      folder_path, 
                                      cnt.VALIDATION_MODEL)
    
        print("Doing prediction with test data...")
        outputs = md.forecast(key, 
                              blob, 
                              date_partition, 
                              run_id,
                              cnt.VALIDATION_MODEL, 
                              cnt.VALIDATION_SEED_DATA,
                              len(df_test))

        helper.plot_figure(\
            key, 
            blob, 
            date_partition, 
            run_id,
            "actual_vs_forecast", 
            [df_test.values, outputs], 
            ["actual", "forecast"], 
            ["Forecast vs Actuals"], 
            use_subplots=False\
        )
        
        # Train model on entire dataset and save model
        print("Training model on entire data...")
        df_train = df_filt.cores.copy()

        _mean, _std = df_train.mean(), df_train.std()
        df_train = (df_train - _mean) / _std

        X_train, y_train = \
            create_dataset(df_train, cnt.IN_STEPS, cnt.OUT_STEPS)
            
        print("Training full model...")
        model:Model = md.model_ts(X_train[0].shape, y_train[0].shape)
        
        md.run_model(\
                    model, 
                    X_train, 
                    y_train, 
                    epochs=100, 
                    batch_size=1024)
        
        print("Saving train data...")
        async_tasks = []
        
        folder_path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.DATA_PATH, 
                                    key)
        
        async_tasks += [asyncio.create_task(\
                            helper.serialize_object_async(\
                                [X_train, y_train], 
                                blob, 
                                folder_path, 
                                cnt.TRAIN_DATA))]  
        
        print("Saving training seed data...")
        async_tasks += [asyncio.create_task(\
                            helper.serialize_object_async(\
                                (X_train[-1], _std, _mean), 
                                blob, 
                                folder_path, 
                                cnt.MODEL_SEED_FILE_NAME))] 
        
        print("Saving full model...")
        folder_path = os.path.join(cnt.FORECAST_ROOT, 
                                    date_partition, 
                                    run_id, 
                                    cnt.MODEL_PATH, 
                                    key)
        
        async_tasks += [asyncio.create_task(\
                            helper.serialize_model_object_async(\
                                        model, 
                                        blob, 
                                        folder_path, 
                                        cnt.FULL_MODEL))]
        
        await asyncio.gather(*async_tasks)