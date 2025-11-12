from google.cloud import storage
import os 
from typing import Optional


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/baothainguyengia/Desktop/[btl]DBMS/gcs_key.json"

class GoogleCloudStorage:
    def __init__(self):
        self.client = storage.Client()
        self.bucket_name = "dbms"
        
        self.bucket = self.ensure_bucket()
        
    def ensure_bucket(self):
        bucket = self.client.bucket(self.bucket_name)
        #bucket = self.client.create_bucket(self.bucket_name)
        return bucket
    
    def upload_bytes(
        self,
        data: bytes,
        destination_blob_name: str,
        content_type: Optional[str] = None,
        make_public: bool = False
    ) -> str:
        """
        Uploads raw bytes to the bucket, returns URI or public URL (if made public).
        """
        blob = self.bucket.blob(destination_blob_name)
        if content_type:
            blob.content_type = content_type
        
        blob.upload_from_string(data, content_type=content_type)
        
        if make_public:
            blob.make_public()
            return blob.public_url
        else:
            # Return gs:// URI
            return f"gs://{self.bucket_name}/{destination_blob_name}"
    

gcs = GoogleCloudStorage()
