"""
  Author                    : mkm
  Description               : IPE DXS Demo Azure Blob upload
  To see the help details   : python3 ./azure_blob_upload.py -h
  Dependency                : azure-storage-blob
  To install dependency     : pip install azure-storage-blob
  To run (Example)          : python3 ./azure_blob_upload.py -au "<account_url>" -st "<account_sas_token>" -bf "<local_folder_path>" -cn "<container_name>"
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
import argparse


argParser = argparse.ArgumentParser(description='Azure Blob information. Dependency: azure-storage-blob')
argParser.add_argument("-au", "--accurl",   dest="acc_url",   type=str, help="Blob service storage account URL")
argParser.add_argument("-st", "--sastoken", dest="sas_token", type=str, help="SAS token of storage account")
argParser.add_argument("-cn", "--containername", dest="container_name", type=str, help="Container name. It should be only in lowercase. default container name:ipecontainer1")
argParser.add_argument("-bf", "--blobfolder", dest="blob_folder", type=str, help="Provide Local folder path of blobs which need to be uploaded to container. Path of single file can also be provided.")

args = argParser.parse_args()

ACCOUNT_URL = args.acc_url
SAS_TOKEN = args.sas_token
CONTAINER_NAME = args.container_name
BLOB_FOLDER = args.blob_folder
print("********************************")
print("ACCOUNT_URL    :",ACCOUNT_URL)
print("SAS_TOKEN      :",SAS_TOKEN)
print("CONTAINER_NAME :",CONTAINER_NAME)
print("BLOB_FOLDER    :",BLOB_FOLDER)
print("******************************** \n")

class IpeAzure(object):
    
    def __init__(self) -> None:
        self.account_url = ACCOUNT_URL
        self.account_key = SAS_TOKEN
        self.container_name = CONTAINER_NAME
        self.local_blob_folder = BLOB_FOLDER
    
    def isBlobLocalFolderExist(self):
        if os.path.exists(self.local_blob_folder):
            return True
        else:
            return False

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
                print(f"{containerName} container already exist, so no need to create it.")
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


    def uploadBlobToContainer(self, containerName):
        
        if not self.isBlobLocalFolderExist():
            print(f"{self.local_blob_folder} not found.")
            return

        blob_name = None
        blob_list = []
        if not os.path.isdir(self.local_blob_folder):
            blob_name = self.local_blob_folder
        else:
            try:
                blob_list = os.listdir(self.local_blob_folder)
                if len(blob_list) == 0:
                    print(f"No local blobs found in the directory {self.local_blob_folder}")
                    return
            except NotADirectoryError:
                print(f"The {self.local_blob_folder} is not a directory.")


        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.account_key)
        container_client = blob_service_client.get_container_client(containerName)
        
        if container_client.exists() == False:
            print(f"{containerName} does not exist")
            container_client.create_container()
            print(f"{containerName} created")
        else:
            pass
            #print(f"Container already exist")

        if not os.path.isdir(self.local_blob_folder):
            blob_name = self.local_blob_folder

        """print(f"blob_list::{blob_list}")
        source_file = blob_list[0]
        source_file = os.path.join(self.local_blob_folder,source_file)
        blob_name = os.path.basename(source_file)
        blob_client = container_client.get_blob_client(blob_name)"""

        if blob_name != None:
            blob_name = os.path.basename(self.local_blob_folder)
            blob_client = container_client.get_blob_client(blob_name)
            if not blob_client.exists():
                self.uploadBlob(blob_client, self.local_blob_folder)
            else:
                print(f'blob {blob_name} already exist in container')
            return
        
        print(f"Local blob list::{blob_list}")
        
        for blob_name in blob_list:
            blob_client = container_client.get_blob_client(blob_name)
            source_blob_path = os.path.join(self.local_blob_folder, blob_name)
            if blob_client.exists():
                print(f'blob {blob_name} already exist in container')
                continue
            else:
                self.uploadBlob(blob_client, source_blob_path)
                
                """stat_info = os.stat(source_file)
                totalSize = stat_info.st_size
                print(f"Total size of the file {source_file} is {totalSize}")

                chunkBytes = 2*1024*1024
                if totalSize < chunkBytes:
                    chunkBytes = totalSize
                
                
                copiedFlag = True
                print(f"Chunk size : {chunkBytes} \n File size : {totalSize}")
                startTime = datetime.now()
                print(f"File Uplaod start time {startTime}")
                uploadedData = 0
                block_list = []
                with open(source_file,"rb") as local_fh:
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
                    print(f"Error while copying.")"""

    def uploadBlob(self,blob_client, source_file):
        stat_info = os.stat(source_file)
        totalSize = stat_info.st_size
        print(f"Total size of the file {source_file} is {totalSize}")

        chunkBytes = 2*1024*1024
        if totalSize < chunkBytes:
            chunkBytes = totalSize
        
        copiedFlag = True
        print(f"Chunk size : {chunkBytes} \n File size : {totalSize}")
        startTime = datetime.now()
        print(f"File Uplaod start time {startTime}")
        uploadedData = 0
        block_list = []
        with open(source_file,"rb") as local_fh:
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
        print(f"file {source_file} is uploaded successfully. Time taken to upload {uploadTime}")

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
                print(f"{containerName} container already exist.")
                retVal = True
        except ResourceNotFoundError as ex:
            print("The container does not exist: {}".format(ex))
            return False
        except HttpResponseError as ex:
            print(f"Verify container exist failed HttpResponseError '{sys.exc_info()[0]}' message.\n")
            return False
        except Exception as ex:
            return False
            pass
        return retVal

if __name__ == '__main__':
    if CONTAINER_NAME == None:
        CONTAINER_NAME = "ipecontainer1"
    containerName = CONTAINER_NAME
    containerName = containerName.lower()
    ipeAzure = IpeAzure()
    if not ipeAzure.verifyIfContainerExist(containerName):
        ipeAzure.createContainer(containerName)
    if not ipeAzure.verifyIfContainerExist(containerName):
        print("Either Account URL or SAS token is invalid. Please verify.")
        exit()
    ipeAzure.uploadBlobToContainer(containerName=containerName)
    ipeAzure.listBlobsInAContainer(containerName=containerName)
    #ipeAzure.deleteContainer(containerName)