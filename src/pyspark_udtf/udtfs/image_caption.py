import requests
import base64
from typing import Iterator, Tuple
from pyspark.sql.functions import udtf
from pyspark.sql.types import Row, StringType, StructType, StructField
from ..utils.version_check import check_version_compatibility

check_version_compatibility("3.5")

class BatchInferenceImageCaptionLogic:
    def __init__(self):
        self.batch_size = 3
        self.endpoint = None
        self.buffer = []
        
    def eval(self, row, batch_size, api_token, endpoint):
        self.batch_size = batch_size
        self.endpoint = endpoint
        self.buffer.append((str(row[0]), api_token))
        if len(self.buffer) >= self.batch_size:
            yield from self.process_batch()
    
    def terminate(self):
        if self.buffer:
            yield from self.process_batch()
    
    def process_batch(self):
        batch_data = self.buffer.copy()
        self.buffer.clear()
        
        # API request timeout in seconds
        api_timeout = 60
        # Maximum tokens for vision model response
        max_response_tokens = 300
        # Temperature controls randomness (lower = more deterministic)
        model_temperature = 0.3
        
        # create a batch for the images
        batch_images = []
        api_token = batch_data[0][1] if batch_data else None
        
        for image_url, _ in batch_data:
            try:
                # Use requests instead of httpx
                image_response = requests.get(image_url, timeout=15)
                image_response.raise_for_status()
                image_data = base64.standard_b64encode(image_response.content).decode("utf-8")
                batch_images.append(image_data)
            except Exception as e:
                raise e
        
        content_items = [{
            "type": "text",
            "text": "Provide brief captions for these images, one per line."
        }]
        for img_data in batch_images:
            content_items.append({
                "type": "image_url",
                "image_url": {
                    "url": "data:image/jpeg;base64," + img_data
                }
            })
        
        payload = {
            "messages": [{
                "role": "user",
                "content": content_items
            }],
            "max_tokens": max_response_tokens,
            "temperature": model_temperature
        }
        
        try:
            response = requests.post(
                self.endpoint,
                headers={
                    'Authorization': 'Bearer ' + api_token,
                    'Content-Type': 'application/json'
                },
                json=payload,
                timeout=api_timeout
            )
            # Check for HTTP errors
            response.raise_for_status()
            
            result = response.json()
            captions = []
            if 'choices' in result:
                 batch_response = result['choices'][0]['message']['content'].strip()
                 lines = batch_response.split('\n')
                 captions = [line.strip() for line in lines if line.strip()]
            
            while len(captions) < len(batch_data):
                captions.append(batch_response if 'batch_response' in locals() else "No caption")
            
        except Exception as e:
            # If API fails, yield error captions for all items in batch
            captions = [f"Error processing batch: {str(e)}" for _ in batch_data]

        for caption in captions[:len(batch_data)]:
            yield Row(caption=caption)

BatchInferenceImageCaption = udtf(
    BatchInferenceImageCaptionLogic, 
    returnType=StructType([StructField("caption", StringType())])
)
