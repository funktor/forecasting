from azure.storage.blob import BlobServiceClient
from azure.storage.blob import aio
import os
from azure.identity import EnvironmentCredential
from pandas import DataFrame
import aiofiles

def get_credentials(app_id:str=None, 
                    tenant_id:str=None, 
                    secret:str=None):
    """
    Get azure credentials to be used with azure python sdk. 
    Uses spot pricing AAD app for authentication.
    
    :param app_id: client app id
    :param tenant_id: client tenant id
    :param secret: app client secret
    :returns: Credential object
    """
    
    os.environ\
    .update({\
        'AZURE_CLIENT_ID':app_id, 
        'AZURE_TENANT_ID':tenant_id, 
        'AZURE_CLIENT_SECRET':secret
    })
        
    return EnvironmentCredential()

class Blob:
    def __init__(self, 
                 storage_account_url, 
                 app_id:str=None, 
                 tenant_id:str=None, 
                 secret:str=None):
        """
        Blob init
        
        :param storage_account_url: blob storage account url
        :param app_id: client app id
        :param tenant_id: client tenant id
        :param secret: app client secret
        """
        credentials = \
            get_credentials(app_id, tenant_id, secret)
            
        self.blob_service_client = \
            BlobServiceClient(\
                account_url=f"https://{storage_account_url}",
                credential=credentials
            )
            
        self.blob_service_client_async = \
            aio.BlobServiceClient(\
                account_url=f"https://{storage_account_url}",
                credential=credentials
            )
        
    def get_blob_client(self, 
                        container:str, 
                        blobPath:str, 
                        _async:bool=False):
        """
        Get blob client
        
        :param container: blob storage container name
        :param blobPath: path of blob w.r.t. container
        :returns: Blob client object
        """       
        if _async:
            return self.blob_service_client_async\
                    .get_blob_client(container, blobPath)
                    
        return self.blob_service_client\
            .get_blob_client(container, blobPath)
    
    def get_container_client(self, 
                             container:str, 
                             _async:bool=False):
        """
        Get blob container client
        
        :param container: blob storage container name
        :returns: Blob container client object
        """ 
        if _async:
            return self.blob_service_client_async\
                    .get_container_client(container)
                 
        return self.blob_service_client\
            .get_container_client(container)

    def get_blob_stream(self, container:str, blobPath:str):
        """
        Read blob stream from blob storage
        
        :param container: blob storage container
        :param blobPath: blob storage file path w.r.t. container
        :returns: Blob stream
        """
        print(f'Getting blob from [{os.path.join(container, blobPath)}]...')
        
        blob_client = self.get_blob_client(container, blobPath)
        
        with blob_client:
            return blob_client.download_blob()
    
    def write_blob_to_local_file(self, container:str, blobPath:str):
        """
        Read blob stream from blob storage and write to local file
        
        :param container: blob storage container
        :param blobPath: blob storage file path w.r.t. container
        """
        try:    
            folder = os.path.dirname(blobPath)
            
            if not os.path.exists(folder):
                os.makedirs(folder) 
                   
            with open(blobPath, 'wb') as f:
                f.write(\
                    self.get_blob_stream(\
                        container,
                        blobPath 
                    ).readall()
                )
        except Exception as e:
            raise Exception

    def get_blob_list_in_container(self, container:str):
        """
        Get list of all blobs in a container

        :param container: The name of the Azure Blob storage container.
        """
        print(f'Getting blob list from [{container}]...')
        
        container_client = self.get_container_client(container)
        
        with container_client:
            blob_list = container_client.list_blobs()
            result = [blob.name for blob in blob_list]
            return result
    
    def initialize_non_existent_container(\
                    self, 
                    output_container:str, 
                    output_file_path:str):
        """
        Setup container and initialize blob client (asynchronous).

        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        container_client = \
            self.get_container_client(output_container)
        
        with container_client:
            if not container_client.exists():
                container_client.create_container()
            
        return self.get_blob_client(output_container, 
                                    output_file_path)

    def upload_file_to_container(self, 
                                 local_file_path:str, 
                                 output_container:str, 
                                 output_file_path:str):
        """
        Uploads a local file to an Azure Blob storage container.

        :param local_file_path: The local path to the file.
        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        blob_client = \
            self.initialize_non_existent_container(\
                output_container, 
                output_file_path)

        print(f'Uploading file {local_file_path} to container [{output_container}]...')

        with blob_client:
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
    
    def upload_str_data_to_container(self, 
                                 data:str, 
                                 output_container:str, 
                                 output_file_path:str):
        """
        Uploads string data to an Azure Blob storage container.

        :param data: string data.
        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        blob_client = \
            self.initialize_non_existent_container(\
                output_container, 
                output_file_path)
            
        print(f'Uploading dataframe to container [{output_container}]...')
        with blob_client:
            blob_client.upload_blob(data, overwrite=True)
        
    def upload_pandas_dataframe_to_container(self, 
                                 df:DataFrame, 
                                 output_container:str, 
                                 output_file_path:str):
        """
        Uploads a pandas dataframe to an Azure Blob storage container.

        :param df: Pandas dataframe.
        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        blob_client = \
            self.initialize_non_existent_container(\
                output_container, 
                output_file_path)
            
        output = df.to_csv(encoding = "utf-8")
        
        print(f'Uploading dataframe to container [{output_container}]...')
        with blob_client:
            blob_client.upload_blob(output, overwrite=True)
        
    async def get_blob_stream_async(self, 
                                    container:str, 
                                    blobPath:str):
        """
        Read blob stream from blob storage (asynchronous)
        
        :param container: blob storage container
        :param blobPath: blob storage file path w.r.t. container
        :returns: Blob stream
        """
        print(f'Getting blob from [{os.path.join(container, blobPath)}]...')
        
        blob_client = self.get_blob_client(container, blobPath, _async=True)
        
        async with blob_client:
            return await blob_client.download_blob()
    
    async def write_blob_to_local_file_async(\
                self, 
                container:str, 
                blobPath:str):
        """
        Read blob stream from blob storage and write to local file (asynchronous)
        
        :param container: blob storage container
        :param blobPath: blob storage file path w.r.t. container
        """
        try:    
            folder = os.path.dirname(blobPath)
            
            if not os.path.exists(folder):
                os.makedirs(folder) 
                   
            async with aiofiles.open(blobPath, 'wb') as f:
                blob_stream = \
                    await self.get_blob_stream_async(container, blobPath)
                data = await blob_stream.readall()
                await f.write(data)
                
        except Exception as e:
            print(e)
            raise Exception
    
    async def initialize_non_existent_container_async(\
                    self, 
                    output_container:str, 
                    output_file_path:str):
        """
        Setup container and initialize blob client (asynchronous).

        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        container_client = self.get_container_client(output_container, _async=True)

        async with container_client: 
            exists = await container_client.exists()
            if not exists:
                await container_client.create_container()
            
        return self.get_blob_client(output_container, output_file_path, _async=True)
            
    async def upload_file_to_container_async(\
                    self, 
                    local_file_path:str, 
                    output_container:str, 
                    output_file_path:str):
        """
        Uploads a local file to an Azure Blob storage container (asynchronous).

        :param local_file_path: The local path to the file.
        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        blob_client = \
            await self.initialize_non_existent_container_async(\
                output_container, 
                output_file_path\
            )
            
        print(f'Uploading file {local_file_path} to container [{output_container}]...')

        async with blob_client:
            with open(local_file_path, "rb") as data:
                await blob_client.upload_blob(data, overwrite=True)
    
    async def upload_str_data_to_container_async(\
                    self, 
                    data:str, 
                    output_container:str, 
                    output_file_path:str):
        """
        Uploads string data to an Azure Blob storage container (asynchronous).

        :param data: string data.
        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        blob_client = \
            await self.initialize_non_existent_container_async(\
                output_container, 
                output_file_path\
            )
            
        print(f'Uploading dataframe to container [{output_container}]...')
        async with blob_client:
            await blob_client.upload_blob(data, overwrite=True)
        
    async def upload_pandas_dataframe_to_container_async(\
                    self, 
                    df:DataFrame, 
                    output_container:str, 
                    output_file_path:str):
        """
        Uploads a pandas dataframe to an Azure Blob storage container (asynchronous).

        :param df: Pandas dataframe.
        :param output_container: The name of the Azure Blob storage container.
        :param output_file_path: The path to the file in blob storage w.r.t. container.
        """
        blob_client = \
            await self.initialize_non_existent_container_async(\
                output_container, 
                output_file_path\
            )
            
        output = df.to_csv(encoding = "utf-8")
        
        print(f'Uploading dataframe to container [{output_container}]...')
        async with blob_client:
            await blob_client.upload_blob(output, overwrite=True)
        
        
        
        
        