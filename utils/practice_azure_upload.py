from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

#My account sas details for ipenf1
#my_sas_token = "?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-31T12:55:44Z&st=2023-02-15T04:55:44Z&spr=https,http&sig=4UL1fLwB%2FDdPfN1Pnv4zES4rXnod40JGhBnghknvOVk%3D"
#acc_service_url = "https://ipenf1.blob.core.windows.net/?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-31T12:55:44Z&st=2023-02-15T04:55:44Z&spr=https,http&sig=4UL1fLwB%2FDdPfN1Pnv4zES4rXnod40JGhBnghknvOVk%3D"
#acc_url = "https://ipenf1.blob.core.windows.net/"

#My account sas details for ipenf1/ipecontainer1
#my_sas_token = "sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"
#container_sas_url = "https://ipenf1.blob.core.windows.net/ipecontainer1?sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"

#My account sas details for ipenf1/ipecontainer1/uploaded_files
#my_sas_token = "sp=racwdlmeop&st=2023-02-15T05:29:07Z&se=2023-03-31T13:29:07Z&sv=2021-06-08&sr=d&sig=Brj%2FhSLD2NtQwaeTjlggIY4o3O2%2Fu8CwbvH%2FnpqmehI%3D&sdd=1"
#blob_sas_url = "https://ipenf1.blob.core.windows.net/ipecontainer1/uploaded_files?sp=racwdlmeop&st=2023-02-15T05:29:07Z&se=2023-03-31T13:29:07Z&sv=2021-06-08&sr=d&sig=Brj%2FhSLD2NtQwaeTjlggIY4o3O2%2Fu8CwbvH%2FnpqmehI%3D&sdd=1"

def list_containers():
    #connection_string = "DefaultEndpointsProtocol=https;AccountName=ipenf1;AccountKey=opIKkPQVXjKoRLbzYT9Gcrt3pz7P1RHtJVrhVBWuZxT2YjLbgPXMkUwlso9a579hOmmadogdbAd/+AStODQ+5g==;EndpointSuffix=core.windows.net"
    connection_string = "BlobEndpoint=https://ipenf1.blob.core.windows.net/;QueueEndpoint=https://ipenf1.queue.core.windows.net/;FileEndpoint=https://ipenf1.file.core.windows.net/;TableEndpoint=https://ipenf1.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-31T12:55:44Z&st=2023-02-15T04:55:44Z&spr=https,http&sig=4UL1fLwB%2FDdPfN1Pnv4zES4rXnod40JGhBnghknvOVk%3D"
    blob_service_client  = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client("mynewcontainer")
    print("container_client list:",container_client.container_name)
    #blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="ipecontainer1", blob_name="my_blob")

def create_BlobServiceClient():
    # Mukesh sas token
    my_sas_token = "sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"
    container_sas_url = "https://ipenf1.blob.core.windows.net/ipecontainer1?sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"
    acc_url = "https://ipenf1.blob.core.windows.net/"

    # Pascal sas token
    #my_sas_token = "sp=racwdli&st=2023-02-14T12:46:37Z&se=2023-12-31T20:46:37Z&spr=https&sv=2021-06-08&sr=c&sig=fw8hGMbLIXcIo8FGbkEU4CZSnEOV%2B%2B3UuTtjOu3Ta18%3D"
    #blob_service_url = "https://ipetestingblob.blob.core.windows.net/testcontainer1?sp=racwdli&st=2023-02-14T12:46:37Z&se=2023-12-31T20:46:37Z&spr=https&sv=2021-06-08&sr=c&sig=fw8hGMbLIXcIo8FGbkEU4CZSnEOV%2B%2B3UuTtjOu3Ta18%3D"
    #acc_url = "https://ipetestingblob.blob.core.windows.net/"

    service = BlobServiceClient(account_url=acc_url, credential=my_sas_token)
    print("service:",service)


#Creating the BlobClient from a SAS URL to a blob.
# SAS url of my account
#sas_url = "https://ipenf1.blob.core.windows.net/?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-31T12:55:44Z&st=2023-02-15T04:55:44Z&spr=https,http&sig=4UL1fLwB%2FDdPfN1Pnv4zES4rXnod40JGhBnghknvOVk%3D"

# SAS url by Pascal
#sas_url = "https://ipetestingblob.blob.core.windows.net/testcontainer1?sp=racwdli&st=2023-02-14T12:46:37Z&se=2023-12-31T20:46:37Z&spr=https&sv=2021-06-08&sr=c&sig=fw8hGMbLIXcIo8FGbkEU4CZSnEOV%2B%2B3UuTtjOu3Ta18%3D"

#blob_client = BlobClient.from_blob_url(sas_url)

"""def uploadData():
    with open("./sftpProcess.py", "rb") as data:
        blob_client.upload_blob(data)
        print(data, ":Data uploaded")
"""

def upload_file():
    # Replace the values below with your own account and container information
    account_url = "https://ipenf1.blob.core.windows.net"
    account_key = "sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"
    container_name = "ipecontainer1"
    blob_name = "IPEmotionRT.log"
    local_file_path = "/mnt/d/IPEmotionRT.log"

    # Create a BlobServiceClient object using the account_url and account_key
    blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)

    # Get a ContainerClient object for the container you want to upload the file to
    container_client = blob_service_client.get_container_client(container_name)

    # Create a BlobClient object for the new blob you want to create
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the file to the blob using the BlobClient object
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
if __name__ == "__main__":
    print("main got called")
    upload_file()
    #list_containers()
    #create_BlobServiceClient()
    print("Uplaod finished")