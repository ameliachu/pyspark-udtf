# Creating Unity Catalog Python UDTFs

This guide explains how to convert a standard PySpark User-Defined Table Function (UDTF) from this repository into a Unity Catalog (UC) registered UDTF.

Reference: [Databricks Documentation: Python UDTFs in Unity Catalog](https://docs.databricks.com/aws/en/udf/udtf-unity-catalog)

## Overview

Unity Catalog UDTFs allow you to register functions that return tables directly in Unity Catalog, making them accessible across your Databricks workspace with governance and permission controls.

## Prerequisites

*   Unity Catalog enabled workspace.
*   Databricks Runtime 17.1+ (for standard access mode) or SQL Warehouse (Serverless/Pro).
*   To use the `pyspark-udtf` package, it must be available to the UC environment (e.g., published to PyPI or available as a workspace library).

## Conversion Steps

To convert a local Python UDTF (like `WriteToMetaCAPI` in `src/pyspark_udtf/udtfs/meta_capi.py`) to a UC UDTF, follow these steps:

### 1. Define the SQL Signature

You need to define the function name, input arguments, and return table schema using SQL DDL.

**Example (`WriteToMetaCAPI`):**

```sql
CREATE OR REPLACE FUNCTION <catalog>.<schema>.meta_capi(
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
```

### 2. Specify Language and Handler

Specify that the function is written in Python and provide the name of the class that implements the logic.

```sql
LANGUAGE PYTHON
HANDLER 'MetaCAPILogic'
```

### 3. Handle Dependencies

There are two main ways to provide the Python code:

#### Option A: Import from Package (Recommended if package is published)

If `pyspark-udtf` is installed in the environment (e.g., via PyPI), you can simply import the class.

```sql
ENVIRONMENT (dependencies = '["pyspark-udtf"]', environment_version = 'None')
AS $$
from pyspark_udtf.udtfs.meta_capi import MetaCAPILogic
$$;
```

#### Option B: Inline Code (Self-contained)

If the package is not available, you must paste the full class definition and any helper classes (like `MappingEngine`) inside the `AS $$ ... $$` block.

```sql
AS $$
# Paste content of mapping_engine.py here (if used)
class MappingEngine:
    ...

# Paste content of meta_capi.py here
class MetaCAPILogic:
    def eval(self, ...):
        ...
$$;
```

### 4. Full Example

Here is the complete SQL command to register the `meta_capi` UDTF, assuming the package is available:

```sql
CREATE OR REPLACE FUNCTION main.default.meta_capi(
  data TABLE,
  pixel_id STRING,
  access_token STRING,
  mapping_yaml STRING,
  test_event_code STRING
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
-- Ensure pyspark-udtf is available in the environment
ENVIRONMENT (dependencies = '["pyspark-udtf"]', environment_version = 'None')
AS $$
from pyspark_udtf.udtfs.meta_capi import MetaCAPILogic
$$;
```

## Key Differences from Standard UDTFs

1.  **Registration**: Standard UDTFs are registered to a `SparkSession` (e.g., `spark.udtf.register`). UC UDTFs are registered to the catalog using SQL DDL.
2.  **Persistence**: UC UDTFs persist across sessions and clusters.
3.  **Table Arguments**: UC UDTFs support `TABLE` arguments in the SQL signature (e.g., `data TABLE`), which allows processing entire input tables.
4.  **Isolation**: UC UDTFs run in isolated environments. You can specify `STRICT ISOLATION` if needed (e.g., for modifying environment variables), though shared isolation is default and more efficient.

## Usage

Once registered, you can use the UDTF in SQL queries.

### Create Input Data

First, create a table with the data you want to send:

```sql
CREATE OR REPLACE TABLE main.default.input_data (
  event_id STRING,
  ts TIMESTAMP,
  email STRING,
  currency STRING,
  value DOUBLE
);

INSERT INTO main.default.input_data VALUES
('evt_1', current_timestamp(), 'user@example.com', 'USD', 100.0),
('evt_2', current_timestamp(), 'another@example.com', 'USD', 55.50);
```

### Call the UDTF

```sql
SELECT * FROM main.default.meta_capi(
  TABLE(main.default.input_data),
  'YOUR_PIXEL_ID',
  'YOUR_ACCESS_TOKEN',
  '
    event_name: "Purchase"
    event_time: 
      source: "ts"
      transform: "to_epoch"
    user_data:
      em: 
        source: "email"
        transform: ["normalize_email", "sha256"]
    custom_data:
      value: "value"
      currency: "currency"
  '
);
```

## Troubleshooting

### Invalid UDTF handler type

If you see an error like `Invalid UDTF handler type. Expected a class (type 'type'), but got an instance of UserDefinedTableFunction`, it means you are using the decorated UDTF wrapper (e.g. `WriteToMetaCAPI` which has `@udtf(...)`) as the handler. Unity Catalog expects the raw Python class.

Ensure you import and use the undecorated class (like `MetaCAPILogic`) in the `HANDLER` and `AS $$` block.
