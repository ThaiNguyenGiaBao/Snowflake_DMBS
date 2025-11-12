import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from io import BytesIO
import logging as log
from typing import Optional, List, Union
from datetime import timedelta
import time
from PIL import Image

load_dotenv()

class MinioService:
    def __init__(self, bucket_name="dbms"):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "localhost")
        self.port = int(os.getenv("MINIO_PORT", 9000))
        self.use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket_name = bucket_name
        
        self.client = Minio(
            f"{self.endpoint}:{self.port}",
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.use_ssl
        )
        
        print(f"Connected to MinIO at {self.endpoint}:{self.port}, SSL: {self.use_ssl}")
        
    def upload_image(self, file_name: str, file_stream: bytes):

        try:
            original = Image.open(BytesIO(file_stream))

        
            img = original.copy()
            buffer = BytesIO()
            img.save(buffer, format="AVIF")  # requires Pillow compiled with AVIF support
            avif_bytes = buffer.getvalue()

            self.client.put_object(
                self.bucket_name,
                f"{file_name}",
                BytesIO(avif_bytes),
                length=len(avif_bytes),
                content_type="image/avif",
            )
            
            return file_name
        except Exception as e:
            log.error("upload_resize_image error: %s", e)
            raise
    
    
    def generate_file_key(self, original_name: str) -> str:
        ts = int(time.time() * 1000)
        # safe replacement of spaces
        safe = original_name.replace(" ", "_")
        return f"{ts}.{safe}"
    
            
            
minio_service = MinioService()

