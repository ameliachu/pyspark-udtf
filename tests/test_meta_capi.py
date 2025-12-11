import pytest
from unittest.mock import Mock, patch
from pyspark.sql.types import Row
from pyspark_udtf.udtfs.meta_capi import MetaCAPILogic

@pytest.fixture
def udtf():
    return MetaCAPILogic()

def test_success(udtf):
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "events_received": 1,
            "fbtrace_id": "trace123"
        }
        mock_post.return_value = mock_response
        
        row = Row(event_payload='{"event_name": "Purchase"}')
        
        # Eval 1 item, batch size is 1000 so it buffers
        res = list(udtf.eval(row, "pixel1", "token1"))
        assert len(res) == 0
        assert len(udtf.buffer) == 1
        
        # Terminate to flush
        results = list(udtf.terminate())
        
        assert len(results) == 1
        # Unpack 5 args: status, received, failed, trace, error
        status, events_rec, events_fail, trace, err = results[0]
        assert status == "success"
        assert events_rec == 1
        assert events_fail == 0
        assert trace == "trace123"
        assert err is None
        
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert kwargs['json']['data'] == [{'event_name': 'Purchase'}]

def test_batching(udtf):
    udtf.batch_size = 2
    
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "events_received": 2,
            "fbtrace_id": "trace123"
        }
        mock_post.return_value = mock_response
        
        row = Row(event_payload='{"e": "test"}')
        
        # 1st item
        list(udtf.eval(row, "p1", "t1"))
        assert len(udtf.buffer) == 1
        
        # 2nd item - should flush
        results = list(udtf.eval(row, "p1", "t1"))
        assert len(results) == 1
        assert len(udtf.buffer) == 0
        status, events_rec, events_fail, _, _ = results[0]
        assert events_rec == 2
        assert events_fail == 0
        
        mock_post.assert_called_once()

def test_credential_change(udtf):
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"events_received": 1}
        mock_post.return_value = mock_response
        
        row1 = Row(event_payload='{"e":1}')
        row2 = Row(event_payload='{"e":2}')
        
        # 1st item
        list(udtf.eval(row1, "p1", "t1"))
        
        # 2nd item different pixel - should flush first
        results = list(udtf.eval(row2, "p2", "t1"))
        
        assert len(results) == 1 # Result from first flush
        assert len(udtf.buffer) == 1 # Buffer has second item
        assert udtf.current_pixel_id == "p2"
        
        mock_post.assert_called_once()
        # Verify call was for p1
        assert "p1" in mock_post.call_args[0][0]

def test_api_failure(udtf):
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "error": {"message": "Bad Request", "fbtrace_id": "err_trace"}
        }
        mock_post.return_value = mock_response
        
        row = Row(event_payload='{"e":1}')
        
        list(udtf.eval(row, "p1", "t1"))
        results = list(udtf.terminate())
        
        assert len(results) == 1
        status, received, failed, trace, err = results[0]
        assert status == "failed"
        assert received == 0
        assert failed == 1
        assert "Bad Request" in err
        assert trace == "err_trace"

def test_invalid_json(udtf):
    row = Row(event_payload="{invalid_json")
    results = list(udtf.eval(row, "p1", "t1"))
    assert len(results) == 1
    status, _, _, _, err = results[0]
    assert status == "failed"
    assert "Invalid payload" in err
