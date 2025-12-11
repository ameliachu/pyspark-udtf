import json
import requests
from pyspark.sql.functions import udtf
from pyspark.sql.types import Row
from typing import Optional, Any

class MetaCAPILogic:
    """
    A PySpark UDTF to send conversion events to the Meta Conversion API (CAPI).
    
    This UDTF accepts a TABLE argument containing event data, buffers the events, 
    and sends them in batches to Meta's Graph API.
    
    Input Arguments:
    - row (Row): Row from the input table. Must contain 'event_payload' (str).
    - pixel_id (str): The Meta Pixel ID.
    - access_token (str): The System User Access Token.
    - test_event_code (str, optional): Code for testing events in Events Manager.
    
    Output Columns:
    - status (str): 'success' or 'failed'.
    - events_received (int): Number of events accepted by Meta.
    - events_failed (int): Number of events failed in this batch.
    - fbtrace_id (str): Trace ID for debugging.
    - error_message (str): Error details if failed.
    """

    def __init__(self):
        self.batch_size = 1000
        self.buffer = []
        # API parameters are set on the first eval call or if they change
        self.current_pixel_id = None
        self.current_access_token = None
        self.current_test_event_code = None
        # API Version
        self.api_version = "v20.0"

    def eval(self, row: Row, pixel_id: str, access_token: str, test_event_code: Optional[str] = None):
        """
        Processes each row from the input table.
        Arguments:
            row: The input row from the table. Expected to have 'event_payload' field.
            pixel_id: Meta Pixel ID (scalar).
            access_token: Access Token (scalar).
            test_event_code: Optional test event code (scalar).
        """
        # If credentials change, flush the existing buffer. 
        if self.buffer and (
            pixel_id != self.current_pixel_id or 
            access_token != self.current_access_token or 
            test_event_code != self.current_test_event_code
        ):
            yield from self._flush()
        
        self.current_pixel_id = pixel_id
        self.current_access_token = access_token
        self.current_test_event_code = test_event_code
        
        # Extract event payload from the row
        if not hasattr(row, 'event_payload'):
             yield "failed", 0, 1, None, "Input table row missing 'event_payload' column."
             return

        event_payload = row.event_payload
        
        try:
            # If payload is already a dict/map in Spark, row.event_payload might be a dict. 
            if isinstance(event_payload, str):
                event_data = json.loads(event_payload)
            else:
                # Assume it's already a dict or Row that can be converted
                if hasattr(event_payload, "asDict"):
                    event_data = event_payload.asDict(recursive=True)
                elif isinstance(event_payload, dict):
                    event_data = event_payload
                else:
                    raise ValueError(f"Unsupported payload type: {type(event_payload)}")

            self.buffer.append(event_data)
        except Exception as e:
            yield "failed", 0, 1, None, f"Invalid payload: {str(e)}"
            return

        if len(self.buffer) >= self.batch_size:
            yield from self._flush()

    def terminate(self):
        if self.buffer:
            yield from self._flush()

    def _flush(self):
        if not self.buffer:
            return

        current_batch_size = len(self.buffer)
        url = f"https://graph.facebook.com/{self.api_version}/{self.current_pixel_id}/events"
        
        params = {"access_token": self.current_access_token}
        
        payload = {
            "data": self.buffer
        }
        
        if self.current_test_event_code:
            payload["test_event_code"] = self.current_test_event_code

        try:
            response = requests.post(url, params=params, json=payload)
            res_json = response.json()
            
            if response.status_code == 200:
                events_received = res_json.get("events_received", 0)
                fbtrace_id = res_json.get("fbtrace_id")
                # Calculate failed as batch_size - events_received (if meaningful), otherwise 0.
                events_failed = max(0, current_batch_size - events_received)
                yield "success", events_received, events_failed, fbtrace_id, None
            else:
                # API returned an error
                error_data = res_json.get("error", {})
                error_msg = error_data.get("message", json.dumps(error_data))
                fbtrace_id = res_json.get("fbtrace_id") or error_data.get("fbtrace_id")
                yield "failed", 0, current_batch_size, fbtrace_id, error_msg
                
        except Exception as e:
            # Network or other exception
            yield "failed", 0, current_batch_size, None, str(e)
        
        # Clear buffer after processing
        self.buffer = []

@udtf(returnType="status: string, events_received: int, events_failed: int, fbtrace_id: string, error_message: string")
class MetaCAPI(MetaCAPILogic):
    """
    Spark UDTF wrapper for MetaCAPILogic.
    """
    pass
