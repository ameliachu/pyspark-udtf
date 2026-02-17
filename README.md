# PySpark UDTF Examples

[![PyPI](https://img.shields.io/pypi/v/pyspark-udtf.svg)](https://pypi.org/project/pyspark-udtf/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)


A collection of Python User-Defined Table Functions (UDTFs) for PySpark, demonstrating how to leverage UDTFs for complex data processing tasks.

## Installation

You can quickly install the package using pip:

```bash
pip install pyspark-udtf
```

## Usage

### Fuzzy Matching (Quick Start)

This UDTF demonstrates how to use Python's standard library `difflib` to perform fuzzy string matching in PySpark. It takes a target string and a list of candidates, returning the best match and a similarity score.

```python
from pyspark.sql import SparkSession
from pyspark_udtf.udtfs import FuzzyMatch

spark = SparkSession.builder.getOrCreate()

# Register the UDTF
spark.udtf.register("fuzzy_match", FuzzyMatch)

# Create a sample dataframe with typos
data = [
    ("aple", ["apple", "banana", "orange"]),
    ("bananna", ["apple", "banana", "orange"]),
    ("orange", ["apple", "banana", "orange"]),
    ("grape", ["apple", "banana", "orange"]) 
]
df = spark.createDataFrame(data, ["typo", "candidates"])

# Use the UDTF in SQL
df.createOrReplaceTempView("typos")

spark.sql("""
    SELECT * 
    FROM fuzzy_match(TABLE(SELECT typo, candidates FROM typos))
""").show()
```

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

## Requirements

- Python >= 3.10
- PySpark >= 4.0.0
- requests
- pandas
- pyarrow

## Documentation

For more detailed documentation, including design docs and guides for Unity Catalog integration, see the [docs/](docs/) directory.

- [Unity Catalog Guide](docs/unity_catalog_udtf.md)

## Development

We recommend using [uv](https://github.com/astral-sh/uv) for extremely fast package management.

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the package
uv add pyspark-udtf
```

### Running Tests

To run the test suite:

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_image_caption.py
```

### Linting

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting. Install dev dependencies, then run:

```bash
uv sync --extra dev   # install ruff
uv run ruff check .   # lint
uv run ruff format .  # format
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

You can bump the version automatically using `uv` (requires uv >= 0.7.0):

```bash
# Bump patch version (0.1.0 -> 0.1.1)
uv version --bump patch

# Bump minor version (0.1.0 -> 0.2.0)
uv version --bump minor
```

Alternatively, you can manually update `pyproject.toml`:

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

## Cursor Skills

This repository includes Cursor skills to help with common development tasks. Skills are available in `.cursor/skills/`.

### create-udtf

Use this skill when you want to **create, write, or generate a new PySpark UDTF**. It guides you through:

1. **Analyze requirements** – Determine inputs, outputs, and external dependencies
2. **Design** – Create a design doc in `docs/design/<udtf_name>.md` (required for all UDTFs)
3. **Implementation** – Implement the UDTF in `src/pyspark_udtf/udtfs/<udtf_name>.py`
4. **Registration** – Add the UDTF to `src/pyspark_udtf/udtfs/__init__.py`
5. **Testing** – Add tests in `tests/test_<udtf_name>.py`

**When to use:** Ask Cursor to create a new UDTF, or say "use the create-udtf skill" when describing the UDTF you want to build.

**Reference implementations:**

- Simple UDTF: `src/pyspark_udtf/udtfs/fuzzy_match.py`
- Complex UDTF (buffering, external API): `src/pyspark_udtf/udtfs/meta_capi.py`
