from pyspark.sql import SparkSession
from pyspark_udtf.udtfs.meta_capi import WriteToMetaCAPI

def main():
    spark = SparkSession.builder \
        .appName("MetaCAPIExample") \
        .getOrCreate()

    # Register the UDTF
    spark.udtf.register("write_to_meta_capi", WriteToMetaCAPI)

    # 1. Create Sample Data
    data = [
        ("O-101", "alice@example.com", "555-0101", 150.0, "USD", "2023-10-27 10:00:00"),
        ("O-102", "bob@example.com", "555-0102", 200.0, "USD", "2023-10-27 10:05:00")
    ]
    columns = ["order_id", "email", "phone", "amount", "currency", "created_at"]
    df = spark.createDataFrame(data, columns)
    df.createOrReplaceTempView("purchases")

    print("Input Data:")
    df.show()

    # 2. Define Mapping Configuration
    # This YAML tells the UDTF how to map columns to Meta CAPI fields
    yaml_mapping = """
    event_name: "Purchase"
    event_time:
      source: "created_at"
      transform: "to_epoch"
    action_source: "website"
    
    user_data:
      em:
        source: "email"
        transform: ["normalize_email", "sha256"]
      ph:
        source: "phone"
        transform: ["normalize_phone", "sha256"]
        
    custom_data:
      value: 
        source: "amount"
        transform: "cast_float"
      currency: 
        source: "currency"
      order_id:
        source: "order_id"
    """

    # 3. Execute UDTF
    # Note: Replace 'YOUR_PIXEL_ID' and 'YOUR_ACCESS_TOKEN' with real values
    # We use a dummy test code 'TEST1234'
    print("Running Meta CAPI UDTF...")
    
    query = f"""
        SELECT * 
        FROM write_to_meta_capi(
            TABLE(purchases),
            '1234567890',     -- pixel_id
            'EAAB_FAKE_TOKEN', -- access_token
            '{yaml_mapping}', -- mapping_yaml
            'TEST1234'        -- test_event_code
        )
    """
    
    result = spark.sql(query)
    result.show(truncate=False)

if __name__ == "__main__":
    main()
