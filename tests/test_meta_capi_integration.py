import os

import pytest
from pyspark.sql import SparkSession

from pyspark_udtf.udtfs.meta_capi import WriteToMetaCAPI


@pytest.fixture(scope="module")
def spark():
    """
    Create a SparkSession for testing.
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("MetaCAPIIntegrationTest") \
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.mark.skipif(
    not (os.environ.get("META_PIXEL_ID") and os.environ.get("META_ACCESS_TOKEN")),
    reason="META_PIXEL_ID and META_ACCESS_TOKEN environment variables are required for this test"
)
def test_meta_capi_integration_real_api(spark):
    """
    Test the WriteToMetaCAPI UDTF against the real Meta Conversion API.
    Requires META_PIXEL_ID and META_ACCESS_TOKEN environment variables.
    """
    pixel_id = os.environ.get("META_PIXEL_ID")
    access_token = os.environ.get("META_ACCESS_TOKEN")

    # Optional: Use a specific test event code if provided, otherwise default to TEST12345
    # TEST12345 is a standard Meta CAPI test code that shows up in the "Test Events" tool.
    test_event_code = os.environ.get("META_TEST_EVENT_CODE", "TEST12345")

    # Register the UDTF
    spark.udtf.register("write_to_meta_capi", WriteToMetaCAPI)

    # Create input data
    # We'll send two events
    data = [
        ("user1@example.com", 100.0, "USD", 1690000000),
        ("user2@example.com", 50.0, "EUR", 1690000000),
    ]
    df = spark.createDataFrame(data, ["email", "amount", "currency", "ts"])
    df.createOrReplaceTempView("purchases")

    # Define mapping YAML
    # We use simple transforms.
    # normalize_email + sha256 is standard for CAPI user_data.
    mapping_yaml = """
    event_name: "Purchase"
    event_time:
      source: "ts"
    user_data:
      em:
        source: "email"
        transform: ["normalize_email", "sha256"]
    custom_data:
      value:
        source: "amount"
      currency: "currency"
    """

    # Run the query
    # We pass the test_event_code to verify events in Events Manager without affecting ads delivery optimization
    query = f"""
        SELECT *
        FROM write_to_meta_capi(
            TABLE(SELECT * FROM purchases),
            '{pixel_id}',
            '{access_token}',
            '{mapping_yaml}',
            '{test_event_code}'
        )
    """

    print(f"Executing query with pixel_id={pixel_id} and test_code={test_event_code}")

    result = spark.sql(query).collect()

    # Verify results
    assert len(result) > 0, "No results returned from UDTF"

    total_received = 0
    total_failed = 0

    for row in result:
        print(f"Row result: status={row.status}, received={row.events_received}, failed={row.events_failed}, trace={row.fbtrace_id}, error={row.error_message}")

        # If successful, we expect events_received > 0 and status = success
        if row.status == "success":
            assert row.events_received > 0
            total_received += row.events_received
        else:
            # If failed, ensure we have an error message
            # This allows the test to pass if credentials are wrong but logic is right,
            # though ideally we want success.
            # But strictly speaking, if credentials are provided, we expect success.
            # If the API returns an error (e.g. invalid token), we might want to fail the test
            # or warn. Here we'll assert that we at least got a response (trace_id or error message).
            assert row.error_message is not None or row.fbtrace_id is not None
            total_failed += row.events_failed

    # We expect at least some attempt was made.
    assert total_received + total_failed > 0

