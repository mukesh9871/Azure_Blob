"""
Dependency:
pip install azure-storage-blob
"""

import os
import sys
from datetime import datetime, timedelta
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, BlobBlock
from azure.storage.blob import ContainerClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from functools import partial
import uuid

# Container details
#ACCOUNT_URL = "https://ipenf1.blob.core.windows.net"
#ACCOUNT_KEY = "sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"
#AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=ipenf1;AccountKey=opIKkPQVXjKoRLbzYT9Gcrt3pz7P1RHtJVrhVBWuZxT2YjLbgPXMkUwlso9a579hOmmadogdbAd/+AStODQ+5g==;EndpointSuffix=core.windows.net"

# ipenf1 account details :: Always use account usr and sas details to connect
ACCOUNT_URL = "https://ipenf1.blob.core.windows.net"
SAS_TOKEN = "?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-04-30T14:24:09Z&st=2023-02-20T06:24:09Z&spr=https,http&sig=4vedpBf25PG03lGQUz0Am55tFy3o7zXu%2FwClE5TdtUc%3D"
AZURE_STORAGE_CONNECTION_STRING = "BlobEndpoint=https://ipenf1.blob.core.windows.net/;QueueEndpoint=https://ipenf1.queue.core.windows.net/;FileEndpoint=https://ipenf1.file.core.windows.net/;TableEndpoint=https://ipenf1.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-04-30T14:24:09Z&st=2023-02-20T06:24:09Z&spr=https,http&sig=4vedpBf25PG03lGQUz0Am55tFy3o7zXu%2FwClE5TdtUc%3D"
BLOB_SAS_URL = "https://ipenf1.blob.core.windows.net/?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-04-30T14:24:09Z&st=2023-02-20T06:24:09Z&spr=https,http&sig=4vedpBf25PG03lGQUz0Am55tFy3o7zXu%2FwClE5TdtUc%3D"

SOURCE_FILE = '/mnt/d/ipeauthmukesh.txt'
#SOURCE_FILE = '/mnt/d/Content.pdf'
#SOURCE_FILE = '/mnt/d/NF-1 readout Station/NF1_Readout_Station_Doc.pdf'
#SOURCE_FILE = '/mnt/d/NF1_backEnd_ReDesign.zip'
#SOURCE_FILE = '/mnt/d/webinterface.zip'
#SOURCE_FILE = '/mnt/d/NF-1 readout Station/Logs/NF1Logs/grpcServer.log.1'
#SOURCE_FILE = '/mnt/d/NF-1 readout Station/Logs/NF1Logs/readoutstationLog.log'

class IpeAzure(object):
    
    def __init__(self) -> None:
        self.account_url = ACCOUNT_URL
        self.account_key = SAS_TOKEN
        
        self.connection_string = AZURE_STORAGE_CONNECTION_STRING
        self.sas_url = BLOB_SAS_URL
    
    def createContainer(self, containerName):
        # Instantiate a BlobServiceClient using a connection string
        # Create blobserviceclient from connection usl
        # blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)

        # Create blob service client from url and sas key
        blob_service_client = BlobServiceClient(self.account_url, self.account_key)

        # Instantiate a ContainerClient with container name
        container_client = blob_service_client.get_container_client(containerName)

        try:
            if not container_client.exists():
                container_client.create_container()
                print(f"{containerName} created")
            else:
                print(f"{containerName} container exist already")
        except ResourceNotFoundError as ex:
            print("The container does not exist: {}".format(ex))
        except HttpResponseError as ex:
            print(f"create container failed HttpResponseError '{sys.exc_info()[0]}' message.\n")
        except:
            print("Unknown exception occured.")
            pass

    def deleteContainer(self, containerName):
        # Instantiate a BlobServiceClient using a connection string
        #blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_service_client = BlobServiceClient(self.account_url, self.account_key)
        container_client = blob_service_client.get_container_client(containerName)
        if container_client.exists():
            container_client.delete_container()
            print(f"container {containerName} deleted successfully.")
        else:
            print(f"container {containerName} does not exist. Nothing to delete.")


    def uplaodBlobToContainer(self, containerName):
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.account_key)
        container_client = blob_service_client.get_container_client(containerName)
        
        if container_client.exists() == False:
            print(f"{containerName} does not exist")
            container_client.create_container()
            print(f"{containerName} created")
        else:
            print(f"Container already exist")

        blob_name = os.path.basename(SOURCE_FILE)
        blob_client = container_client.get_blob_client(blob_name)

        stat_info = os.stat(SOURCE_FILE)
        totalSize = stat_info.st_size
        print(f"Total size of the file {SOURCE_FILE} is {totalSize}")

        chunkBytes = 2*1024*1024
        if totalSize < chunkBytes:
            chunkBytes = totalSize
        
        
        copiedFlag = True
        print(f"Chunk size : {chunkBytes} \n File size : {totalSize}")
        startTime = datetime.now()
        print(f"File Uplaod start time {startTime}")
        uploadedData = 0
        block_list = []
        with open(SOURCE_FILE,"rb") as local_fh:
            while True:
                read_data = local_fh.read(chunkBytes)
                if not read_data:
                    break
                chunk_id = str(uuid.uuid4())
                blob_client.stage_block(chunk_id, read_data)
                block_list.append(BlobBlock(chunk_id))
                uploadedData = uploadedData + len(read_data)
                uploadPercentage = (uploadedData * 100)/totalSize
                print(f"Data uploaded till now {uploadedData} Percentage =", round(uploadPercentage,2),"%")
        try:
            blob_client.commit_block_list(block_list)
        except:
            print(f"Error while copying.")

        endTime = datetime.now()
        print(f"File upload end time {endTime}")
        uploadTime = endTime - startTime
        print(f"file {SOURCE_FILE} is uploaded successfully. Time taken to upload {uploadTime}")

        """copiedFlag = True
        print(f"Chunk size : {chunkBytes} \n File size : {totalSize}")
        startTime = datetime.now()
        print(f"File Uplaod start time {startTime}")
        uploadedData = 0
        with open(SOURCE_FILE,"rb") as local_fh:
            for data in iter(partial(local_fh.read, chunkBytes), b''):
                try:
                    blob_client.upload_blob(data, overwrite=False)
                    print("Chunk uploaded")
                    uploadedData = uploadedData + len(data)
                    uploadPercentage = (uploadedData * 100)/totalSize
                    print(f"Data uploaded till now {uploadedData} Percentage =", round(uploadPercentage,2),"%")
                except:
                    copiedFlag = False
                    break
                
            if not copiedFlag:
                print(f"Error while copying.")
                return False

        endTime = datetime.now()
        print(f"File upload end time {endTime}")
        uploadTime = endTime - startTime
        print(f"file {SOURCE_FILE} is uploaded successfully. Time taken to upload {uploadTime}")"""

        """startTime = datetime.now()
        print(f"start time {startTime}")
        with open(SOURCE_FILE, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        endTime = datetime.now()
        print(f"end time {endTime}")
        uploadTime = endTime - startTime

        print(f"file {SOURCE_FILE} is uploaded successfully. Time taken to upload {uploadTime}")"""


    def listBlobsInAContainer(self, containerName):
        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.account_key)

        # Instantiate a ContainerClient
        container_client = blob_service_client.get_container_client(containerName)

        # Verify if the container does exist
        try:
            if not container_client.exists():
                print(f"{containerName} container does not exist")
                return
        except:
            pass

        blobs_list = container_client.list_blobs()
        print("\n"+f"List of blobs in the container {container_client.container_name}:")
        for blob in blobs_list:
            print(blob.name)


    def verifyIfContainerExist(self, containerName):
        blob_service_client = BlobServiceClient(self.account_url, self.account_key)
        container_client = blob_service_client.get_container_client(containerName)
        retVal = False
        try:
            if container_client.exists() == False:
                print(f"{containerName} does not exist")
                retVal = False
            else:
                print(f"{containerName} container exist already")
                retVal = True
        except ResourceNotFoundError as ex:
            print("The container does not exist: {}".format(ex))
        except HttpResponseError as ex:
            print(f"create container failed HttpResponseError '{sys.exc_info()[0]}' message.\n")
        except Exception as ex:
            pass
        return retVal

if __name__ == '__main__':
    containerName = "ipecontainer1"
    ipeAzure = IpeAzure()
    ipeAzure.verifyIfContainerExist(containerName)
    ipeAzure.createContainer(containerName)
    ipeAzure.uplaodBlobToContainer(containerName=containerName)
    ipeAzure.listBlobsInAContainer(containerName=containerName)
    #ipeAzure.deleteContainer(containerName)