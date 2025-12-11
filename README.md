# PySpark UDTF Examples

A collection of Python User-Defined Table Functions (UDTFs) for PySpark, demonstrating how to leverage UDTFs for complex data processing tasks.

## Requirements

- Python >= 3.10
- PySpark >= 4.0.0
- requests
- pandas
- pyarrow

## Installation

```bash
pip install pyspark-udtf
```

## Usage

### Batch Inference Image Captioning

This UDTF demonstrates how to perform efficient batch inference against a model serving endpoint. It buffers rows and sends them in batches to reduce network overhead.

```python
from pyspark.sql import SparkSession
from pyspark_udtf.udtfs import BatchInferenceImageCaption

spark = SparkSession.builder.getOrCreate()

# Register the UDTF
spark.udtf.register("batch_image_caption", BatchInferenceImageCaption)

# View UDTF definition and parameters
help(BatchInferenceImageCaption.func)

# Usage in SQL
# Assuming you have a table 'images' with a column 'url'
spark.sql("""
    SELECT * 
    FROM batch_image_caption(
        TABLE(SELECT url FROM images), 
        10,  -- batch_size
        'your-api-token', 
        'https://your-endpoint.com/score'
    )
""").show()
```

#### Arrow-Optimized Version (PySpark 4.2+)

If you are using PySpark 4.2 or later, you can use the Arrow-optimized version of the UDTF for better performance.

```python
try:
    from pyspark_udtf.udtfs import ArrowBatchInferenceImageCaption
    spark.udtf.register("arrow_batch_image_caption", ArrowBatchInferenceImageCaption)
    
    # Usage is identical
    spark.sql("""
        SELECT * 
        FROM arrow_batch_image_caption(
            TABLE(SELECT url FROM images), 
            10, 'token', 'endpoint'
        )
    """).show()
except ImportError:
    print("Arrow UDTF not available (requires PySpark 4.2+)")
```

## Available UDTFs

### `BatchInferenceImageCaption`

Performs batch inference for image captioning.

**Arguments (SQL):**
1. `TABLE(input)`: The input table containing image URLs.
2. `batch_size` (int): Number of images to process in a single batch.
3. `token` (str): API token for authentication.
4. `endpoint` (str): The model serving endpoint URL.

**Input:**
- A table with image URLs (column name maps to the first argument of the eval method).

**Output:**
- A struct containing the `caption` (string).

### `ArrowBatchInferenceImageCaption` (PySpark 4.2+)

Same as above, but uses Apache Arrow for data transfer, providing improved performance.

## Development

1. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

2. Run tests:
   ```bash
   pytest
   ```
