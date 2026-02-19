from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import Row

from pyspark_udtf.udtfs.image_caption import BatchInferenceImageCaptionLogic


@pytest.fixture
def udtf_instance():
    return BatchInferenceImageCaptionLogic()

def test_eval_buffering(udtf_instance):
    # Test that eval buffers and doesn't yield until batch size is met

    # First item - should be buffered
    # Pass a Row object to simulate real Spark behavior
    row1 = Row(url="http://image1.jpg")
    results1 = list(udtf_instance.eval(row1, 2, "fake-token", "http://fake-endpoint"))
    assert len(results1) == 0
    assert len(udtf_instance.buffer) == 1
    assert udtf_instance.batch_size == 2
    assert udtf_instance.buffer[0] == ("http://image1.jpg", "fake-token")

    # Second item - should trigger processing
    row2 = Row(url="http://image2.jpg")

    with patch('requests.post') as mock_post, \
         patch('requests.get') as mock_get:

        # Mock image download
        mock_img_response = Mock()
        mock_img_response.content = b"fake-image-data"
        mock_img_response.raise_for_status.return_value = None
        mock_get.return_value = mock_img_response

        # Mock API response (OpenAI format)
        mock_response = Mock()
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'caption1\ncaption2'
                }
            }]
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        results2 = list(udtf_instance.eval(row2, 2, "fake-token", "http://fake-endpoint"))

        assert len(results2) == 2
        assert results2[0].caption == 'caption1'
        assert results2[1].caption == 'caption2'
        assert len(udtf_instance.buffer) == 0 # Buffer should be cleared

        mock_post.assert_called_once()
        # Verify payload structure
        call_args = mock_post.call_args[1]
        assert 'messages' in call_args['json']
        assert len(call_args['json']['messages'][0]['content']) == 3 # 1 text + 2 images

def test_terminate_flushes_buffer(udtf_instance):
    # Test that terminate processes remaining items

    # Add one item (less than batch size of 2)
    row1 = Row(url="http://image1.jpg")
    list(udtf_instance.eval(row1, 2, "fake-token", "http://fake-endpoint"))
    assert len(udtf_instance.buffer) == 1

    with patch('requests.post') as mock_post, \
         patch('requests.get') as mock_get:

        mock_img_response = Mock()
        mock_img_response.content = b"fake-image-data"
        mock_img_response.raise_for_status.return_value = None
        mock_get.return_value = mock_img_response

        mock_response = Mock()
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'caption1'
                }
            }]
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        results = list(udtf_instance.terminate())

        assert len(results) == 1
        assert results[0].caption == 'caption1'
        assert len(udtf_instance.buffer) == 0

        mock_post.assert_called_once()

def test_error_handling(udtf_instance):
    # Test that API errors are handled gracefully

    # Initialize params
    udtf_instance.batch_size = 2
    # Buffer is set manually below

    with patch('requests.post') as mock_post, \
         patch('requests.get') as mock_get:

        mock_img_response = Mock()
        mock_img_response.content = b"fake-image-data"
        mock_img_response.raise_for_status.return_value = None
        mock_get.return_value = mock_img_response

        mock_post.side_effect = Exception("API Error")

        # Trigger batch processing immediately by setting buffer manually
        udtf_instance.buffer = [("http://image1.jpg", "t"), ("http://image2.jpg", "t")]

        results = list(udtf_instance.process_batch())

        assert len(results) == 2
        assert "Error processing batch" in results[0].caption
        assert "Error processing batch" in results[1].caption
        assert len(udtf_instance.buffer) == 0 # Buffer should still be cleared
