import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import httpx
from utils.fetcher import fetch_generation_data, fetch_single_chunk, validate_data_quality

class TestFetcher:
    @patch('utils.fetcher.httpx.Client')
    def test_fetch_single_chunk_success(self, mock_client):
        mock_response = Mock()
        mock_response.json.return_value = {"data": [{"psrType": "Solar", "quantity": 100.5}]}
        mock_response.raise_for_status.return_value = None
        
        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client.return_value.__enter__.return_value = mock_client_instance
        
        result = fetch_single_chunk(datetime(2024, 1, 1), datetime(2024, 1, 1))
        assert len(result) == 1
        assert result[0]["psrType"] == "Solar"

    def test_validate_data_quality_valid(self):
        data = [{"settlementDate": "2024-01-01", "psrType": "Solar", "quantity": 100}]
        result = validate_data_quality(data)
        assert result["status"] == "success"