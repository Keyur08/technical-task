import pytest
from datetime import datetime
from utils.preprocessing import (
    deduplicate_data,
    handle_missing_fields,
    validate_processed_data
)


class TestPreprocessing:
    """Test cases for data preprocessing utilities."""
    
    def test_deduplicate_data_empty_list(self):
        """Test deduplication with empty list."""
        result = deduplicate_data([])
        assert result == []
    
    def test_deduplicate_data_no_duplicates(self):
        """Test deduplication with no duplicate records."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5,
                "publishTime": "2024-01-01T00:00:00Z"
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 2,
                "psrType": "Solar",
                "quantity": 110.0,
                "publishTime": "2024-01-01T00:30:00Z"
            }
        ]
        
        result = deduplicate_data(data)
        assert len(result) == 2
        assert result == data
    
    def test_deduplicate_data_with_duplicates(self):
        """Test deduplication removes older duplicate records."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5,
                "publishTime": "2024-01-01T00:00:00Z"
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 105.0,
                "publishTime": "2024-01-01T01:00:00Z"  # Later timestamp
            }
        ]
        
        result = deduplicate_data(data)
        assert len(result) == 1
        assert result[0]["quantity"] == 105.0  # Keep the later record
        assert result[0]["publishTime"] == "2024-01-01T01:00:00Z"
    
    def test_deduplicate_data_multiple_fuel_types(self):
        """Test deduplication with multiple fuel types."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5,
                "publishTime": "2024-01-01T00:00:00Z"
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Wind Onshore",
                "quantity": 200.0,
                "publishTime": "2024-01-01T00:00:00Z"
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 110.0,
                "publishTime": "2024-01-01T01:00:00Z"
            }
        ]
        
        result = deduplicate_data(data)
        assert len(result) == 2
        # Check that we have both fuel types
        fuel_types = [record["psrType"] for record in result]
        assert "Solar" in fuel_types
        assert "Wind Onshore" in fuel_types
        # Check that Solar record is the later one
        solar_record = next(r for r in result if r["psrType"] == "Solar")
        assert solar_record["quantity"] == 110.0
    
    def test_handle_missing_fields_empty_list(self):
        """Test handling missing fields with empty list."""
        result = handle_missing_fields([])
        assert result == []
    
    def test_handle_missing_fields_complete_record(self):
        """Test handling missing fields with complete record."""
        data = [
            {
                "publishTime": "2024-01-01T00:00:00Z",
                "businessType": "A75",
                "psrType": "Solar",
                "quantity": 100.5,
                "startTime": "2024-01-01T00:00:00Z",
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "fuelType": "Solar",
                "region": "GB"
            }
        ]
        
        result = handle_missing_fields(data)
        assert len(result) == 1
        assert result[0] == data[0]
    
    def test_handle_missing_fields_partial_record(self):
        """Test handling missing fields with partial record."""
        data = [
            {
                "psrType": "Solar",
                "quantity": 100.5,
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1
                # Missing other fields
            }
        ]
        
        result = handle_missing_fields(data)
        assert len(result) == 1
        assert result[0]["psrType"] == "Solar"
        assert result[0]["quantity"] == 100.5
        assert result[0]["publishTime"] is None
        assert result[0]["businessType"] is None
        assert result[0]["fuelType"] is None
        assert result[0]["region"] is None
    
    def test_handle_missing_fields_extra_fields(self):
        """Test handling missing fields preserves extra fields."""
        data = [
            {
                "psrType": "Solar",
                "quantity": 100.5,
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "extraField": "extra_value"
            }
        ]
        
        result = handle_missing_fields(data)
        assert len(result) == 1
        assert result[0]["extraField"] == "extra_value"
    
    def test_validate_processed_data_empty_list(self):
        """Test validation with empty data."""
        result = validate_processed_data([])
        assert result["status"] == "error"
        assert "No data to validate" in result["message"]
    
    def test_validate_processed_data_valid_data(self):
        """Test validation with valid processed data."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5,
                "publishTime": "2024-01-01T00:00:00Z"
            },
            {
                "settlementDate": "2024-01-02",
                "settlementPeriod": 1,
                "psrType": "Wind Onshore",
                "quantity": 150.0,
                "publishTime": "2024-01-02T00:00:00Z"
            }
        ]
        
        result = validate_processed_data(data)
        
        assert result["status"] == "success"
        assert result["record_count"] == 2
        assert "Solar" in result["fuel_types"]
        assert "Wind Onshore" in result["fuel_types"]
        assert result["date_range"]["min"] == "2024-01-01"
        assert result["date_range"]["max"] == "2024-01-02"
        assert result["date_range"]["unique_dates"] == 2
        assert result["data_quality_score"] == 100.0
        assert result["records_with_missing_critical"] == 0
    
    def test_validate_processed_data_missing_critical_fields(self):
        """Test validation with missing critical fields."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": None,  # Missing critical field
                "quantity": 100.5
            },
            {
                "settlementDate": None,  # Missing critical field
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": None  # Missing critical field
            }
        ]
        
        result = validate_processed_data(data)
        
        assert result["status"] == "success"
        assert result["record_count"] == 2
        assert result["records_with_missing_critical"] == 2
        assert result["data_quality_score"] == 0.0  # Both records have missing critical fields
        
        # Check missing stats
        assert "psrType" in result["missing_stats"]
        assert "settlementDate" in result["missing_stats"]
        assert "quantity" in result["missing_stats"]
        assert result["missing_stats"]["psrType"]["count"] == 1
        assert result["missing_stats"]["settlementDate"]["count"] == 1
        assert result["missing_stats"]["quantity"]["count"] == 1
    
    def test_validate_processed_data_partial_missing(self):
        """Test validation with partially missing fields."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5
            },
            {
                "settlementDate": "2024-01-02",
                "settlementPeriod": None,  # Missing field
                "psrType": "Wind Onshore",
                "quantity": 150.0
            }
        ]
        
        result = validate_processed_data(data)
        
        assert result["status"] == "success"
        assert result["record_count"] == 2
        assert result["records_with_missing_critical"] == 1
        assert result["data_quality_score"] == 50.0  # 1 out of 2 records complete
        assert result["missing_stats"]["settlementPeriod"]["count"] == 1
        assert result["missing_stats"]["settlementPeriod"]["percentage"] == 50.0
    
    def test_validate_processed_data_quantity_statistics(self):
        """Test validation calculates correct quantity statistics."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.0
            },
            {
                "settlementDate": "2024-01-02",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 200.0
            },
            {
                "settlementDate": "2024-01-03",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 300.0
            }
        ]
        
        result = validate_processed_data(data)
        
        assert result["status"] == "success"
        assert result["quantity_stats"]["min"] == 100.0
        assert result["quantity_stats"]["max"] == 300.0
        assert result["quantity_stats"]["avg"] == 200.0
        assert result["quantity_stats"]["count"] == 3
    
    def test_validate_processed_data_no_quantities(self):
        """Test validation with no valid quantities."""
        data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": None
            }
        ]
        
        result = validate_processed_data(data)
        
        assert result["status"] == "success"
        assert result["quantity_stats"] == {}


class TestPreprocessingIntegration:
    """Integration tests for preprocessing pipeline."""
    
    def test_full_preprocessing_pipeline(self):
        """Test complete preprocessing pipeline."""
        # Simulate raw data with duplicates and missing fields
        raw_data = [
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5,
                "publishTime": "2024-01-01T00:00:00Z"
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 105.0,
                "publishTime": "2024-01-01T01:00:00Z"  # Later timestamp - should be kept
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": 2,
                "psrType": "Wind Onshore",
                "quantity": 200.0
                # Missing publishTime and other fields
            }
        ]
        
        # Step 1: Deduplicate
        deduplicated = deduplicate_data(raw_data)
        assert len(deduplicated) == 2
        
        # Verify Solar record is the later one
        solar_record = next(r for r in deduplicated if r["psrType"] == "Solar")
        assert solar_record["quantity"] == 105.0
        
        # Step 2: Handle missing fields
        processed = handle_missing_fields(deduplicated)
        assert len(processed) == 2
        
        # Verify missing fields are handled
        wind_record = next(r for r in processed if r["psrType"] == "Wind Onshore")
        assert wind_record["publishTime"] is None
        assert wind_record["businessType"] is None
        
        # Step 3: Validate processed data
        validation = validate_processed_data(processed)
        assert validation["status"] == "success"
        assert validation["record_count"] == 2
        assert validation["data_quality_score"] == 100.0  # All critical fields present
        assert len(validation["fuel_types"]) == 2
    
    def test_preprocessing_with_poor_quality_data(self):
        """Test preprocessing with poor quality input data."""
        poor_data = [
            {
                "settlementDate": None,  # Missing critical field
                "settlementPeriod": 1,
                "psrType": "Solar",
                "quantity": 100.5
            },
            {
                "settlementDate": "2024-01-01",
                "settlementPeriod": None,  # Missing critical field
                "psrType": None,  # Missing critical field
                "quantity": 150.0
            },
            {
                # Valid record
                "settlementDate": "2024-01-02",
                "settlementPeriod": 1,
                "psrType": "Wind Offshore",
                "quantity": 300.0
            }
        ]
        
        # Process through pipeline
        deduplicated = deduplicate_data(poor_data)
        processed = handle_missing_fields(deduplicated)
        validation = validate_processed_data(processed)
        
        assert validation["status"] == "success"
        assert validation["record_count"] == 3
        assert validation["records_with_missing_critical"] == 2
        assert validation["data_quality_score"] == 33.33  # 1 out of 3 records complete
        assert len(validation["missing_stats"]) >= 2


if __name__ == "__main__":
    # pytest tests/test_preprocessing.py -v
    pass