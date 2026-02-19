
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """
    Create a SparkSession for testing.
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("UDTFTest") \
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_batch_image_caption_integration(spark):
    """
    Test the UDTF in a real Spark session.
    """

    # Register the UDTF
    # spark.udtf.register("batch_image_caption", BatchInferenceImageCaption)

    # Create input data
    data = [
        ("http://example.com/1.jpg",),
        ("http://example.com/2.jpg",),
        ("http://example.com/3.jpg",),
    ]
    df = spark.createDataFrame(data, ["url"])
    df.createOrReplaceTempView("images")

    # Mocking for integration test
    from collections.abc import Iterator

    from pyspark.sql.functions import udtf
    from pyspark.sql.types import Row, StringType, StructField, StructType

    from pyspark_udtf.udtfs.image_caption import BatchInferenceImageCaptionLogic

    class MockedBatchInferenceImageCaption(BatchInferenceImageCaptionLogic):
        def process_batch(self) -> Iterator[Row]:
            # Simulate API response
            for _i, (url, _) in enumerate(self.buffer):
                yield Row(caption=f"caption for {url}")
            self.buffer = []

    # Register the mocked UDTF
    MockedUDTF = udtf(MockedBatchInferenceImageCaption, returnType=StructType([StructField("caption", StringType())]))
    spark.udtf.register("mocked_batch_image_caption", MockedUDTF)

    # Run the query
    result = spark.sql("""
        SELECT *
        FROM mocked_batch_image_caption(
            TABLE(SELECT url FROM images),
            2,
            'fake-token',
            'http://fake-endpoint'
        )
    """).collect()

    # Verify results
    assert len(result) == 3
    captions = sorted([r.caption for r in result])
    expected = [
        "caption for http://example.com/1.jpg",
        "caption for http://example.com/2.jpg",
        "caption for http://example.com/3.jpg"
    ]
    assert captions == expected
