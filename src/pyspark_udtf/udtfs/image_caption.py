import requests
import base64
from typing import Iterator, Tuple
from pyspark.sql.functions import udtf
from pyspark.sql.types import Row, StringType, StructType, StructField
from ..utils.version_check import check_version_compatibility

class BatchInferenceImageCaptionLogic:
    """
    A UDTF that generates image captions using batch inference.
    
    Args:
        batch_size (int): Number of images to process in a single batch.
        token (str): API token for authentication.
        endpoint (str): The model serving endpoint URL.
    """
    
    def __init__(self):
        self.batch_size = None
        self.token = None
        self.endpoint = None
        self.buffer = []

    def eval(self, row: Row, batch_size: int, token: str, endpoint: str):
        """
        Processes each row. Buffers the image URL and triggers batch processing
        when the buffer size reaches the configured batch_size.
        """
        if self.batch_size is None:
            self.batch_size = batch_size
            self.token = token
            self.endpoint = endpoint
        
        # Handle input row: assume the first column is the URL
        if hasattr(row, "__getitem__") and not isinstance(row, str):
            url = row[0]
        else:
            url = row

        self.buffer.append(url)
        if len(self.buffer) >= self.batch_size:
            yield from self.process_batch()

    def terminate(self):
        """
        Called when all rows have been processed.
        Processes any remaining items in the buffer.
        """
        if self.buffer:
            yield from self.process_batch()

    def process_batch(self) -> Iterator[Row]:
        """
        sends a batch of images to the model serving endpoint.
        """
        if not self.buffer:
            return

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            # Simplified payload structure matching common serving endpoints
            payload = {
                "inputs": self.buffer
            }
            
            response = requests.post(
                self.endpoint,
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            
            # Assuming response JSON contains a list of predictions/captions
            predictions = response.json().get('predictions', [])
            
            if len(predictions) != len(self.buffer):
                # Fallback if lengths mismatch
                for _ in self.buffer:
                    yield Row(caption="Error: API response mismatch")
            else:
                for caption in predictions:
                    yield Row(caption=str(caption))
                    
        except Exception as e:
            error_msg = f"Error processing batch: {str(e)}"
            for _ in self.buffer:
                yield Row(caption=error_msg)
        finally:
            self.buffer = []

# Standard UDTF registration (Works on PySpark 4.0+)
BatchInferenceImageCaption = udtf(
    BatchInferenceImageCaptionLogic, 
    returnType=StructType([StructField("caption", StringType())])
)

# Optional Arrow UDTF registration (Requires PySpark 4.2+)
try:
    from pyspark.sql.functions import arrow_udtf
    ArrowBatchInferenceImageCaption = arrow_udtf(
        BatchInferenceImageCaptionLogic, 
        returnType=StructType([StructField("caption", StringType())])
    )
except ImportError:
    # If arrow_udtf is missing, check if it's due to old PySpark version
    if not check_version_compatibility("4.2"):
        # We silently skip defining the Arrow variant on older versions
        # to maintain compatibility with PySpark 4.0/4.1.
        # If we were strictly enforcing this feature, we would raise an exception here.
        pass
    else:
        # If version is >= 4.2 but import failed, re-raise
        raise
