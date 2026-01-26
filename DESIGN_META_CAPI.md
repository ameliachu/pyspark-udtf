# Meta Conversion API (CAPI) UDTF Design (v2: Mapping Support)

## Objective
Enable users to send conversion data to Meta CAPI using a **standard, normalized input table** (instead of pre-formatted JSON). A **YAML mapping configuration** defines how input columns are transformed into the Meta CAPI payload structure.

## Overview
- **Input:** 
  - A Spark Table (DataFrame) with arbitrary columns (e.g., `email`, `order_value`, `ts`).
  - A YAML string defining the mapping from columns to CAPI fields.
  - API Credentials (pixel_id, access_token).
- **Process:** 
  - The UDTF reads the YAML configuration.
  - For each row, it applies the mapping (including hashing/normalization) to construct the event payload.
  - Events are buffered and batched (max 1000/batch).
  - Batches are sent to Meta Graph API.
- **Output:**
  - Status and metrics per batch (success/fail counts, trace IDs, errors).

## UDTF Signature

```python
write_to_meta_capi(
    TABLE(input_data),              # The input DataFrame
    pixel_id: str,                  # Meta Pixel ID
    access_token: str,              # System User Access Token
    mapping_yaml: str,              # YAML string defining the column mapping
    test_event_code: str = None     # Optional test code
)
```

## YAML Mapping Schema

The YAML configuration drives the transformation. It supports literals, column references, and transformations (like hashing).

### Structure
```yaml
# Top-level fields map directly to CAPI event fields
event_name: <MappingRule>
event_time: <MappingRule>
action_source: <MappingRule>

# Nested objects
user_data:
  em: <MappingRule>  # email
  ph: <MappingRule>  # phone
  # ... other user_data fields

custom_data:
  value: <MappingRule>
  currency: <MappingRule>
  # ... other custom_data fields
```

### MappingRule Types

1.  **Literal**: A static value for all rows.
    ```yaml
    event_name: "Purchase"
    # OR explicit syntax
    event_name:
      type: literal
      value: "Purchase"
    ```

2.  **Column Reference**: Value comes from a specific column.
    ```yaml
    event_id: "order_id"  # specific column name implies string implies literal unless nested. 
    # WAIT: Simple string value in YAML is treated as LITERAL in our implementation. 
    # To use a column, you MUST use the explicit source syntax.
    event_id:
      source: "order_id"
    ```

3.  **Transformation**: Apply functions like hashing or casting.
    ```yaml
    user_data:
      em:
        source: "email_raw"
        transform: ["normalize", "sha256"] # Applied in order
    
    event_time:
      source: "timestamp"
      transform: "to_epoch"
    ```

### Supported Transformations
*   `sha256`: Computes SHA-256 hash (hex string).
*   `normalize`: Trims whitespace and converts to lowercase.
*   `to_epoch`: Converts Spark Timestamp/Date or ISO string to Unix epoch integer.
*   `cast_int`: Casts to integer.
*   `cast_float`: Casts to float.

## Example Usage

### 1. Input Table (`purchases`)
| order_id | email | amount | currency | created_at |
| :--- | :--- | :--- | :--- | :--- |
| "O-101" | "alice@example.com" | 150.0 | "USD" | 2023-10-27 10:00:00 |

### 2. YAML Configuration
```yaml
event_name: "Purchase"
event_time:
  source: "created_at"
  transform: "to_epoch"
action_source: "website"

user_data:
  em:
    source: "email"
    transform: ["normalize", "sha256"]

custom_data:
  value: 
    source: "amount"
    transform: "cast_float"
  currency: "currency"  # This is a literal "currency" string
  order_id:
    source: "order_id"
```

### 3. Spark SQL Call
```python
from pyspark_udtf.udtfs.meta_capi import WriteToMetaCAPI
spark.udtf.register("write_to_meta_capi", WriteToMetaCAPI)

# Note: YAML must be passed as a single string literal.
yaml_mapping = """
event_name: "Purchase"
event_time:
  source: "created_at"
  transform: "to_epoch"
...
"""

spark.sql(f"""
    SELECT * 
    FROM write_to_meta_capi(
        TABLE(purchases),
        '1234567890',
        'EAAB...',
        '{yaml_mapping}',
        'TEST1234'
    )
""")
```

## Internal Logic Updates

1.  **Initialization (`__init__` / `eval` start)**:
    -   Parse `mapping_yaml` (using `PyYAML` or internal parser).
    -   Validate that referenced columns exist in the input row.

2.  **Row Processing (`eval`)**:
    -   Iterate through the mapping configuration.
    -   Resolve values from `row`.
    -   Apply transformations.
    -   Construct the dictionary payload.
    -   Buffer the result.

3.  **Dependencies**:
    -   Add `PyYAML` to `pyproject.toml`.

## Output Schema (Unchanged)
| status | events_received | events_failed | fbtrace_id | error_message |
| :--- | :--- | :--- | :--- | :--- |
| string | int | int | string | string |
