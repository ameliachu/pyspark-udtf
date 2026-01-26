import pytest
import json
from unittest.mock import Mock, patch
from pyspark.sql.types import Row
from pyspark_udtf.udtfs.mapping_engine import MappingEngine
from pyspark_udtf.udtfs.meta_capi import MetaCAPILogic, WriteToMetaCAPI

# --- Mapping Engine Tests ---

def test_mapping_engine_basic():
    yaml_config = """
    event_name: "Purchase"
    event_time: 
      source: "ts"
      transform: "to_epoch"
    user_data:
      em: 
        source: "email"
        transform: ["normalize_email", "sha256"]
    """
    engine = MappingEngine(yaml_config)
    row = Row(ts="1600000000", email="  Test@Example.com  ")
    
    result = engine.transform_row(row)
    
    assert result['event_name'] == "Purchase"
    # to_epoch on an int string should return int
    assert result['event_time'] == 1600000000
    
    # Check email hashing: normalize -> "test@example.com" -> sha256
    expected_hash = "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b"
    assert result['user_data']['em'] == expected_hash

    def test_mapping_engine_nested():
        yaml_config = """
        custom_data:
          value: 
            source: "amount"
          currency: "currency"
        """
        engine = MappingEngine(yaml_config)
        row = Row(amount=100.50, currency="USD")
    
        result = engine.transform_row(row)
        assert result['custom_data']['value'] == 100.50
        assert result['custom_data']['currency'] == "USD"

def test_mapping_engine_transforms():
    yaml_config = """
    val_int: 
      source: "v"
      transform: "cast_int"
    val_float:
      source: "v"
      transform: "cast_float"
    val_phone:
      source: "phone"
      transform: "normalize_phone"
    """
    engine = MappingEngine(yaml_config)
    row = Row(v="123", phone="(555) 123-4567")
    
    result = engine.transform_row(row)
    assert result['val_int'] == 123
    assert isinstance(result['val_int'], int)
    assert result['val_float'] == 123.0
    assert isinstance(result['val_float'], float)
    assert result['val_phone'] == "5551234567"

# --- UDTF Flow Tests ---

@pytest.fixture
def udtf():
    return MetaCAPILogic()

def test_udtf_with_mapping(udtf):
    mapping_yaml = """
    event_name: "Purchase"
    custom_data:
      value: 
        source: "amount"
    """
    
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "events_received": 1,
            "fbtrace_id": "trace123"
        }
        mock_post.return_value = mock_response
        
        row = Row(amount=50.0)
        
        # Eval 1 item
        list(udtf.eval(row, "pixel1", "token1", mapping_yaml))
        
        # Terminate
        results = list(udtf.terminate())
        
        assert len(results) == 1
        status, received, failed, trace, err = results[0]
        assert status == "success"
        
        # Check payload construction
        mock_post.assert_called_once()
        call_json = mock_post.call_args[1]['json']
        event = call_json['data'][0]
        
        assert event['event_name'] == "Purchase"
        assert event['custom_data']['value'] == 50.0

def test_udtf_mapping_change(udtf):
    # If mapping yaml changes, buffer should flush and engine update
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"events_received": 1}
        mock_post.return_value = mock_response
        
        mapping1 = 'event_name: "A"'
        mapping2 = 'event_name: "B"'
        
        row = Row()
        
        # Batch size 1000
        list(udtf.eval(row, "p1", "t1", mapping1)) # Buffer 1
        assert len(udtf.buffer) == 1
        assert udtf.buffer[0]['event_name'] == "A"
        
        # Change mapping -> should flush first batch, then start new buffer
        results = list(udtf.eval(row, "p1", "t1", mapping2)) 
        
        assert len(results) == 1 # Flushed batch 1
        assert len(udtf.buffer) == 1 # Buffer now has batch 2 item
        assert udtf.buffer[0]['event_name'] == "B"

def test_udtf_mapping_error(udtf):
    # Test that mapping error (e.g. missing column) handles gracefully
    # We must use explicit source syntax to trigger column lookup failure
    mapping_yaml = """
    event_id: 
      source: "missing_column"
    """
    
    row = Row(existing="val")
    
    results = list(udtf.eval(row, "p1", "t1", mapping_yaml))
    
    assert len(results) == 1
    status, _, failed, _, err = results[0]
    assert status == "failed"
    assert "Mapping error" in err
    # PySpark 4.0+ error message
    assert "ATTRIBUTE_NOT_SUPPORTED" in err or "AttributeError" in err or "has no attribute" in err
