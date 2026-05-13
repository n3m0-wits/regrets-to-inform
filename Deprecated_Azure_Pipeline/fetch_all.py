import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()
ACC_NAME= 'deepstatedatabase'
ACC_KEY = os.environ.get('ADLS_ACCOUNT_KEY')
CONN_STR = os.environ.get("deepstatedatabase_STORAGE")

def start_download(container_name = "cleanmail", local_folder = "email_dump/", adls_folder = 'synced'):
    blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
    container_client = blob_service_client.get_container_client(container = container_name)

    print("Starting bulk download...")
    for blob in container_client.list_blobs(name_starts_with=f"{adls_folder}/"):
        blob_client = container_client.get_blob_client(blob.name)
        local_path = os.path.join(f"{local_folder}", blob.name)
        if os.path.exists(local_path):
            print(f"Skipped (already exists): {blob.name}")
            continue
        # Create directories and download only if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
            print(f"Downloaded: {blob.name}")

    print("Downloads complete!")
    return
# Actual Download Script
def main():
    container_name = "cleanmail"
    local_folder = "unprocessed_email_dump/"
    adls_folder = 'unprocessed'
    start_download(container_name, local_folder, adls_folder)
    return


if __name__ == "__main__":
    main()