# PySpark UDTF Examples

[![PyPI](https://img.shields.io/pypi/v/pyspark-udtf.svg)](https://pypi.org/project/pyspark-udtf/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)


A collection of Python User-Defined Table Functions (UDTFs) for PySpark, demonstrating how to leverage UDTFs for complex data processing tasks.

## Requirements

- Python >= 3.10
- PySpark >= 4.0.0
- requests
- pandas
- pyarrow

## Installation

We recommend using [uv](https://github.com/astral-sh/uv) for extremely fast package management.

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the package
uv add pyspark-udtf
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

## Documentation

For more detailed documentation, including design docs and guides for Unity Catalog integration, see the [docs/](docs/) directory.

- [Unity Catalog Guide](docs/unity_catalog_udtf.md)

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and packaging.

### Running Tests

To run the test suite:

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_image_caption.py
```

### Adding Dependencies

To add a new runtime dependency:

```bash
uv add package_name
```

To add a development dependency:

```bash
uv add --dev package_name
```

### Bumping Version

Currently, versioning is managed manually in `pyproject.toml`.

1. Open `pyproject.toml`.
2. Update the `version` field under `[project]`:
   ```toml
   [project]
   version = "0.1.1"  # Update this value
   ```

### Publishing to PyPI

To build and publish the package to PyPI:

1. **Build the package:**
   ```bash
   uv build
   ```
   This will create distributions in the `dist/` directory.

2. **Publish to PyPI:**
   ```bash
   uv publish
   ```
   Note: You will need to configure your PyPI credentials (API token) either via environment variables (`UV_PUBLISH_TOKEN`) or following `uv`'s authentication documentation.
