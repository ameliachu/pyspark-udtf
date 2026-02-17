import json
from importlib.metadata import PackageNotFoundError, version as _pkg_version
import requests
from pyspark.sql.functions import udtf
from pyspark.sql.types import Row
from typing import Optional, Any

from .mapping_engine import MappingEngine


def _partner_agent() -> str:
    try:
        return f"pyspark-udtf/{_pkg_version('pyspark-udtf')}"
    except PackageNotFoundError:
        return "pyspark-udtf/unknown"


# Resolve once at import; used in CAPI payload.
_PARTNER_AGENT = _partner_agent()

class MetaCAPILogic:
    """
    A PySpark UDTF to send conversion events to the Meta Conversion API (CAPI).
    
    This UDTF accepts a TABLE argument containing event data, buffers the events, 
    and sends them in batches to Meta's Graph API.
    
    Input Arguments:
    - row (Row): Row from the input table.
    - pixel_id (str): The Meta Pixel ID.
    - access_token (str): The System User Access Token.
    - mapping_yaml (str): YAML string defining the mapping from input columns to CAPI event fields.
    - test_event_code (str, optional): Code for testing events in Events Manager.
    
    Output Columns:
    - status (str): 'success' or 'failed'.
    - events_received (int): Number of events accepted by Meta.
    - events_failed (int): Number of events failed in this batch.
    - fbtrace_id (str): Trace ID for debugging.
    - error_message (str): Error details if failed.

    Example Usage:
    
    ```python
    from pyspark_udtf.udtfs.meta_capi import WriteToMetaCAPI
    spark.udtf.register("write_to_meta_capi", WriteToMetaCAPI)
    
    yaml_mapping = '''
    event_name: "Purchase"
    event_time: 
      source: "ts"
      transform: "to_epoch"
    user_data:
      em: 
        source: "email"
        transform: ["normalize_email", "sha256"]
    '''
    
    spark.sql(f'''
        SELECT * 
        FROM write_to_meta_capi(
            TABLE(input_df), 
            'YOUR_PIXEL_ID', 
            'YOUR_ACCESS_TOKEN', 
            '{yaml_mapping}'
        )
    ''')
    ```

    Unity Catalog Registration:

    ```sql
    CREATE OR REPLACE FUNCTION meta_capi(
      data TABLE, 
      pixel_id STRING, 
      access_token STRING, 
      mapping_yaml STRING, 
      test_event_code STRING DEFAULT NULL
    )
    RETURNS TABLE (
      status STRING, 
      events_received INT, 
      events_failed INT, 
      fbtrace_id STRING, 
      error_message STRING
    )
    LANGUAGE PYTHON
    HANDLER 'MetaCAPILogic'
    ENVIRONMENT (dependencies = '["pyspark-udtf"]', environment_version = 'None')
    AS $$
    from pyspark_udtf.udtfs.meta_capi import MetaCAPILogic
    $$;
    ```
    """

    def __init__(self):
        self.batch_size = 1000
        self.buffer = []
        # API parameters are set on the first eval call or if they change
        self.current_pixel_id = None
        self.current_access_token = None
        self.current_mapping_yaml = None
        self.current_test_event_code = None
        
        self.mapping_engine = None
        # API Version
        self.api_version = "v20.0"

    def eval(self, row: Row, pixel_id: str, access_token: str, mapping_yaml: str, test_event_code: Optional[str] = None):
        """
        Processes each row from the input table.
        """
        # If credentials or mapping change, flush the existing buffer. 
        if self.buffer and (
            pixel_id != self.current_pixel_id or 
            access_token != self.current_access_token or 
            mapping_yaml != self.current_mapping_yaml or
            test_event_code != self.current_test_event_code
        ):
            yield from self._flush()
        
        # Initialize or update mapping engine if yaml changed
        if mapping_yaml != self.current_mapping_yaml:
            self.mapping_engine = MappingEngine(mapping_yaml)
            self.current_mapping_yaml = mapping_yaml
        
        self.current_pixel_id = pixel_id
        self.current_access_token = access_token
        self.current_test_event_code = test_event_code
        
        try:
            # Use MappingEngine to transform row
            event_data = self.mapping_engine.transform_row(row)
            
            if event_data:
                self.buffer.append(event_data)
        except Exception as e:
            # Yield failure for specific row mapping error
            yield "failed", 0, 1, None, f"Mapping error: {str(e)}"
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
            "data": self.buffer,
            "partner_agent": _PARTNER_AGENT
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
                error_user_title = error_data.get("error_user_title")
                error_user_msg = error_data.get("error_user_msg")
                error_subcode = error_data.get("error_subcode")
                fbtrace_id = res_json.get("fbtrace_id") or error_data.get("fbtrace_id")
                
                # Build detailed error message
                detailed_error_msg = f"[{response.status_code}] {error_msg}"
                if error_user_title:
                    detailed_error_msg += f": {error_user_title}"
                if error_user_msg:
                    detailed_error_msg += f" - {error_user_msg}"
                if error_subcode:
                    detailed_error_msg += f" | Subcode: {error_subcode}"
                
                yield "failed", 0, current_batch_size, fbtrace_id, detailed_error_msg
                
        except Exception as e:
            # Network or other exception
            yield "failed", 0, current_batch_size, None, str(e)
        
        # Clear buffer after processing
        self.buffer = []

@udtf(returnType="status: string, events_received: int, events_failed: int, fbtrace_id: string, error_message: string")
class WriteToMetaCAPI(MetaCAPILogic):
    """
    Spark UDTF wrapper for MetaCAPILogic.
    """
    pass

