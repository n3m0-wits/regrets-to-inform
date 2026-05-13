import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Replace with your actual connection string from local.settings.json
conn_str = os.getenv("deepstatedatabase_STORAGE")

container_name = "cleanmail"
local_folder = "output_files"

blob_service_client = BlobServiceClient.from_connection_string(conn_str)
container_client = blob_service_client.get_container_client(container_name)

# print("Starting bulk upload...")
# for filename in os.listdir(local_folder):
#     if filename.endswith(".json"):
#         local_path = os.path.join(local_folder, filename)
#         blob_name = f"unprocessed/{filename}"
        
#         print(f"Uploading {filename}...")
#         blob_client = container_client.get_blob_client(blob_name)
        
#         with open(local_path, "rb") as data:
#             blob_client.upload_blob(data, overwrite=True)

# print("Upload complete!")

def move_blobs(source_prefix, target_prefix):
    blobs = container_client.list_blobs(name_starts_with=source_prefix)
    
    for blob in blobs:
        # Define new path
        relative_path = blob.name[len(source_prefix):]
        new_name = target_prefix + relative_path
        
        # Start copy
        source_blob_url = f"{container_client.url}/{blob.name}"
        new_blob_client = container_client.get_blob_client(new_name)
        new_blob_client.start_copy_from_url(source_blob_url)
        
        # Delete original
        container_client.delete_blob(blob.name)
        print(f"Moved: {blob.name} -> {new_name}")

# # Execute the moves
# move_blobs("unprocessed/processed/", "unprocessed/")
# move_blobs("unprocessed/synced/", "unprocessed/")
# # move_blobs("processed/", "unprocessed/")