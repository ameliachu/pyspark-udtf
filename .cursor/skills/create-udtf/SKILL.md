---
name: create-udtf
description: Generates PySpark User-Defined Table Functions (UDTFs) from descriptions, including design docs for complex logic, implementation, and tests. Use when the user wants to create, write, or generate a new UDTF.
---

# Create PySpark UDTF

## Workflow

Follow this workflow to create a high-quality PySpark UDTF.

### 1. Analyze Requirements

Understand the user's request. Determine the inputs, outputs, and necessary external dependencies.

### 2. Design (Required)

Create a design document in `docs/design/<udtf_name>.md`. **This is required for ALL UDTFs.**

*   **Content**:
    *   **Overview**: Brief description of what the UDTF does.
    *   **Usage Example**: A **copy-pastable** Python snippet demonstrating how to register and call the UDTF.
        *   **CRITICAL**: All variables in the example (e.g., input DataFrames, schema strings, mapping configurations) MUST be defined in the snippet. Do not use undefined variables like `df` or `schema` without showing their creation.
        *   **Secrets**: Use placeholders for secrets (e.g., `'YOUR_API_KEY'`).
    *   **API Specification**: detailed Input Arguments and Output Schema.
    *   **Behavior**: High-level description of buffering, state management, or API interaction.
*   **Constraints**:
    *   **Avoid Implementation Details**: Do not write the full code or complex internal logic in the design doc. Focus on the interface and behavior.
*   **Review**: Ask the user to review the design before coding.

### 3. Implementation

Create the UDTF file in `src/pyspark_udtf/udtfs/<udtf_name>.py`.

*   **Reference**: Check `src/pyspark_udtf/udtfs/fuzzy_match.py` (simple) or `src/pyspark_udtf/udtfs/meta_capi.py` (complex) for examples.
*   **Best Practices**:
    *   Use type hints.
    *   **Analyze Method**: Only implement `analyze` if absolutely necessary (e.g., polymorphic UDTFs where output schema cannot be determined statically). Prefer static `returnType` in `@udtf` decorator for most cases.
    *   Use `yield` to emit rows.
    *   Handle `TABLE` arguments (rows) efficiently.
    *   Use `terminate` for flushing buffers or cleaning up resources.
    *   **Self-Contained**: Keep the implementation in a single file if possible. Avoid creating separate utility files unless the logic is shared across multiple UDTFs or is extremely complex.
    *   **Docstring**: Include the **copy-pastable usage example** from the design doc in the class docstring.
    *   **Note**: This skill is **NOT** for creating Unity Catalog (UC) Python UDTFs using `CREATE FUNCTION` syntax. A separate skill handles UC registration. Focus solely on the Python class implementation here.

### 4. Registration

Register the new UDTF in `src/pyspark_udtf/udtfs/__init__.py` to make it importable.

### 5. Testing

Create a test file in `tests/test_<udtf_name>.py`.

*   **Reference**: Check `tests/test_image_caption.py` or `tests/test_meta_capi.py` for testing patterns.
*   **Requirements**:
    *   Use `pytest`.
    *   Test `eval` logic directly (unit test).
    *   Test `analyze` logic if implemented.
    *   Mock external dependencies (e.g., `requests`) if used.
    *   Verify output schema and data correctness.

## Best Practices & Context

*   **Project Context**: Read `.cursor/rules/project_context.mdc` for project-specific rules.
*   **Documentation**:
    *   [PySpark UDTF Docs](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_udtf.html)
    *   [Databricks UDTF Docs](https://docs.databricks.com/aws/en/udf/python-udtf)
*   **Table Arguments**: Prefer `TABLE` arguments for processing entire tables/partitions.
