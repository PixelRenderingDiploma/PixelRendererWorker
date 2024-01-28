import base64
import json
import requests
from dataclasses import dataclass, field
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage

CONNERCTION_STRING = "DefaultEndpointsProtocol=https;AccountName=pixelrenderingstorage;AccountKey=Unp8Muly8GPMmN24Oc61wbCwBCv+EpObRuhUf9mAUiPHCYnm9+ws12HVnTmlkTRo5WQPDYqNZ6MT+AStLFlKzQ==;EndpointSuffix=core.windows.net"
ACCESS_STORAGE_ENDPOINT_URL_FOR_ACCOUNT = 'https://pixelrenderer-azurefunctions.azurewebsites.net'
QUEUE_NAME = "renderingqueuedebug"

@dataclass
class AzureManager:
    queue_client: QueueClient = field(init=False)

    def __post_init__(self):
        self.queue_client = QueueClient.from_connection_string(CONNERCTION_STRING, QUEUE_NAME)

    def get_user_blob_sas_url(self, blob_path: str, id_token: str) -> str: 
        endpoint = f"{ACCESS_STORAGE_ENDPOINT_URL_FOR_ACCOUNT}/api/GetUserBlobSasUrl?blobPath={blob_path}"

        header = { "Authorization": f"Bearer {id_token}" }

        response = requests.get(endpoint, headers=header)
        sasUrl = response.text

        return sasUrl
    
    def download_file_from_azure_storage(self, api_url, file_path):
        download_response = requests.get(api_url)

        if download_response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(download_response.content)
        
        return download_response.status_code
    
    def upload_file_to_azure_storage(self, api_url, data: bytes):
        header = { 'x-ms-blob-type': 'BlockBlob' }
        
        return requests.put(api_url, data=data, headers=header).status_code
    
    def get_media(self, file_path, blob_path, id_token):
        blob_sas_url = self.get_user_blob_sas_url(blob_path, id_token)

        print('Downloading file: ', blob_path)
        resp = self.download_file_from_azure_storage(blob_sas_url, file_path)
        print('File downloaded with code: ', resp)

        return resp

    def put_media_data(self, data: bytes, blob_path, id_token):
        blob_sas_url = self.get_user_blob_sas_url(blob_path, id_token)

        print('Uploading file: ', blob_path)
        resp = self.upload_file_to_azure_storage(blob_sas_url, data)
        print('File uploaded with code: ', resp)

        return resp
    
    def put_media_path(self, file_path: str, blob_path, id_token):
        with open(file_path, 'rb') as file:
            data = file.read()
            self.put_media_data(data, blob_path, id_token)

    def delete_media(self, blob_path, id_token: str) -> bool:
        blob_sas_url = self.get_user_blob_sas_url(blob_path, id_token)
        response = requests.delete(blob_sas_url)

        # Check if the deletion was successful (status code 202)
        if response.status_code == 202:
            return True
        else:
            return False
    
    def get_next_config(self):
        message = self.queue_client.receive_message(visibility_timeout=3)

        if message:
            configs = json.loads(message.content)

            self.queue_client.delete_message(message)

            return configs