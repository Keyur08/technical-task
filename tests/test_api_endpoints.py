import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from datetime import date
import tempfile
import os

from main import app
from database.operations import DatabaseOperations
from utils.visualization import DataVisualizer


class TestHealthEndpoint:
    """Test cases for health endpoint."""
    
    def test_health_check_success(self):
        """Test successful health check."""
        with TestClient(app) as client:
            response = client.get("/api/v1/health")
            assert response.status_code == 200


class TestDataEndpoints:
    """Test cases for data-related endpoints with proper mocking."""
    
    def test_retrieve_data_success(self):
        """Test successful data retrieval."""
        mock_record = Mock()
        mock_record.settlement_date = date(2024, 1, 1)
        mock_record.settlement_period = 1
        mock_record.psr_type = "Solar"
        mock_record.quantity = 100.5
        mock_record.fuel_type = "Solar"
        mock_record.region = "GB"
        mock_record.publish_time = None

        with patch('api.routes.data.get_db_operations') as mock_get_db_ops:
            mock_db_ops = Mock(spec=DatabaseOperations)
            mock_db_ops.get_data.return_value = [mock_record]
            mock_get_db_ops.return_value = mock_db_ops

            with TestClient(app) as client:
                response = client.post("/api/v1/data/retrieve", json={
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "fuel_types": ["Solar"],
                    "limit": 100
                })

                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "success"
                assert data["count"] == 1
                assert len(data["data"]) == 1
                assert data["data"][0]["psr_type"] == "Solar"

    def test_retrieve_data_no_filters(self):
        """Test data retrieval without filters."""
        with patch('api.routes.data.get_db_operations') as mock_get_db_ops:
            mock_db_ops = Mock(spec=DatabaseOperations)
            mock_db_ops.get_data.return_value = []
            mock_get_db_ops.return_value = mock_db_ops

            with TestClient(app) as client:
                response = client.post("/api/v1/data/retrieve", json={})

                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "success"
                assert data["count"] == 0

    def test_get_summary_stats(self):
        """Test getting summary statistics."""
        expected_summary = {
            "total_records": 1000,
            "unique_dates": 30,
            "fuel_type_breakdown": [
                {
                    "fuel_type": "Solar",
                    "count": 500,
                    "avg_quantity": 150.0,
                    "total_quantity": 75000.0
                }
            ],
            "date_range": {
                "min": "2024-01-01",
                "max": "2024-01-31"
            }
        }

        with patch('api.routes.data.get_db_operations') as mock_get_db_ops:
            mock_db_ops = Mock(spec=DatabaseOperations)
            mock_db_ops.get_summary_stats.return_value = expected_summary
            mock_get_db_ops.return_value = mock_db_ops

            with TestClient(app) as client:
                response = client.get("/api/v1/data/summary")

                assert response.status_code == 200
                data = response.json()
                assert data["total_records"] == 1000
                assert data["fuel_type_breakdown"][0]["fuel_type"] == "Solar"

    def test_clear_data_with_confirmation(self):
        """Test clearing data with confirmation."""
        with patch('api.routes.data.get_db_operations') as mock_get_db_ops:
            mock_db_ops = Mock(spec=DatabaseOperations)
            mock_db_ops.clear_all_data.return_value = {"deleted_count": 500}
            mock_get_db_ops.return_value = mock_db_ops

            with TestClient(app) as client:
                response = client.delete("/api/v1/data/clear?confirm=true")

                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "success"
                assert "Cleared 500 records" in data["message"]


class TestPlotEndpoints:
    """Test cases for plot generation endpoints."""
    
    def test_generate_daily_plot(self):
        """Test generating daily plot."""
        with patch('api.routes.plots.get_visualizer') as mock_get_visualizer:
            mock_visualizer = Mock(spec=DataVisualizer)
            mock_fig = Mock()
            mock_visualizer.create_daily_generation_plot.return_value = mock_fig
            mock_get_visualizer.return_value = mock_visualizer

            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
                temp_path = temp_file.name

            try:
                with TestClient(app) as client:
                    response = client.post("/api/v1/plots/generate", json={
                        "plot_type": "daily",
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "title": "Test Daily Plot"
                    })

                    assert response.status_code == 200
                    assert response.headers["content-type"] == "image/png"
                    
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

    def test_generate_plot_invalid_type(self):
        """Test generating plot with invalid type."""
        with TestClient(app) as client:
            response = client.post("/api/v1/plots/generate", json={
                "plot_type": "invalid_type"
            })
            
            assert response.status_code == 422  # Validation error


# Run only these fixed tests
if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])