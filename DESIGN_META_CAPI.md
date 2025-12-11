# Meta Conversion API (CAPI) UDTF Design

## Objective
Enable users to send conversion data securely and seamlessly to Meta's Conversion API (CAPI) directly from PySpark using a User Defined Table Function (UDTF). This allows for efficient, scalable recurring workflows for activating 1st party data.

## Meta CAPI Overview
- **Endpoint:** `POST https://graph.facebook.com/{API_VERSION}/{PIXEL_ID}/events`
- **Payload Structure:**
  ```json
  {
    "data": [
        { "event_name": "Purchase", "event_time": 1600000000, ... }
    ],
    "access_token": "..."
  }
  ```

## UDTF Specification

### Class Name
`MetaCAPI`

### Input Arguments
The UDTF follows the Spark UDTF `TABLE` argument convention to accept an input relation.

1.  **row** (`Row`): Represents a row from the input table. The input table **must** contain a column named `event_payload` (string containing JSON) or valid columns that can be constructed into the payload. For simplicity, we assume `event_payload`.
2.  **pixel_id** (`str`): The Meta Pixel ID (scalar).
3.  **access_token** (`str`): The System User Access Token (scalar).
4.  **test_event_code** (`str`, optional): A code used to verify events in the Events Manager "Test Events" tab (scalar).

### Output Schema
The UDTF outputs the result of each API batch call.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `status` | `string` | API call status ("success", "failed", "partial"). |
| `events_received` | `int` | Number of events successfully processed by Meta. |
| `events_failed` | `int` | Number of events failed (e.g., due to network error or API rejection). |
| `fbtrace_id` | `string` | Unique identifier for the request, useful for debugging. |
| `error_message` | `string` | Error details if the request failed. |

### Internal Logic
1.  **Buffering:** The `eval` method receives rows from the input table. It extracts the `event_payload` and buffers it.
2.  **Batching:** Meta CAPI accepts up to 1000 events per request. The UDTF flushes the buffer when it reaches a configured batch size (e.g., 1000) or when `terminate` is called.
3.  **Execution:**
    -   Sends a POST request to `https://graph.facebook.com/{version}/{pixel_id}/events`.
    -   Handles authentication via `access_token`.
4.  **Response Parsing:**
    -   On 200 OK: Parses `events_received` and `fbtrace_id`. `events_failed` is 0.
    -   On Error: Captures error message. `events_received` is 0. `events_failed` is the batch size.

### Example Usage

The UDTF is invoked using the `TABLE` keyword to pass the input dataset. This allows Spark to partition the data efficiently (e.g., `PARTITION BY` if we had multiple pixels, though here we pass pixel_id as scalar).

```python
from pyspark_udtf.udtfs.meta_capi import MetaCAPI

# Register UDTF
spark.udtf.register("meta_capi", MetaCAPI)

# 1. Create a DataFrame with the event payloads
df_events = spark.table("conversions").selectExpr(
    "to_json(struct(event_name, event_time, user_data, custom_data)) as event_payload"
)

# 2. Call UDTF using SQL syntax with TABLE argument
# Arguments: TABLE(input), pixel_id, access_token, test_event_code
spark.sql("""
    SELECT * 
    FROM meta_capi(
        TABLE(events_view), 
        '1234567890', 
        'EAAB...', 
        'TEST1234'
    )
""")
```

### Batching & Partitioning Strategy
-   By using `TABLE(df)`, Spark feeds rows to the UDTF `eval` method.
-   The UDTF handles batching internally (accumulating rows until 1000).
-   If multiple workers are used, each worker processes a partition of the input table and sends batches independently.

### Monitoring
-   Users can aggregate the output table to get total `events_received` vs `events_failed`.
-   Errors are preserved in `error_message` for debugging.
