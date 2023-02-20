from azure.storage.blob import BlobClient
from azure.storage.blob._download import StorageStreamDownloader
import os
from azure.storage.blob import BlobServiceClient

# Define your storage account and container information
ACCOUNT_URL = "https://ipenf1.blob.core.windows.net"
ACCOUNT_KEY = "sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"

account_name = "ipenf1"
account_key = "sp=racwdlmeop&st=2023-02-15T05:24:19Z&se=2023-03-31T13:24:19Z&sv=2021-06-08&sr=c&sig=OApjhedN0q4RusQXmlYUaNmrkxIfvyOScsMFORP%2F1ZA%3D"
container_name = "ipecontainer1"
local_file_path = '/mnt/d/NF-1 readout Station/Logs/NF1Logs/grpcServer.log.1'
blob_name = os.path.basename(local_file_path)

# Define the block size (in bytes) for the upload
block_size = 4 * 1024 * 1024  # 4 MB

# Create a BlockBlobService object using the account name and key
block_blob_service = BlobServiceClient(account_url=ACCOUNT_URL, credential=ACCOUNT_KEY)

# Get the size of the file
file_size = os.path.getsize(local_file_path)

# Create the blob with an initial empty block list
#block_blob_service.create_blob_from_path(container_name, blob_name, local_file_path, max_connections=1)
#block_list = block_blob_service.get_block_list(container_name, blob_name, 'uncommitted')
block_ids = 0

# Upload the file in blocks
with open(local_file_path, 'rb') as file:
    for i in range(0, file_size, block_size):
        # Read the next block from the file
        block_data = file.read(block_size)
        block_id = '{:08d}'.format(i // block_size)

        # Upload the block to the blob
        block_blob_service.put_block(container_name, blob_name, block_data, block_id)

        # Add the block ID to the list of uncommitted blocks
        block_ids.append(block_id)

        # Calculate and print the upload progress
        uploaded_size = min(i + block_size, file_size)
        percentage = uploaded_size / file_size * 100
        print("{:.2f}% complete".format(percentage))
        block_ids = block_ids + 1

# Commit the blocks to the blob
block_blob_service.put_block_list(container_name, blob_name, block_ids)

# Print the URL of the uploaded blob
blob_url = block_blob_service.make_blob_url(container_name, blob_name)
print("Uploaded blob URL: {}".format(blob_url))