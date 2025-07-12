# utils/preprocessing.py
import logging
from typing import List, Dict
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

def deduplicate_data(data: List[dict]) -> List[dict]:
    """Remove duplicate records, keeping the most recent publishTime."""
    if not data:
        return []
    
    logger.info(f"Starting deduplication on {len(data)} records")
    
    record_groups = defaultdict(list)
    
    for record in data:
        key = (
            record.get("settlementDate"),
            record.get("settlementPeriod"),
            record.get("psrType")
        )
        record_groups[key].append(record)
    
    deduplicated_data = []
    total_duplicates = 0
    
    for key, records in record_groups.items():
        if len(records) > 1:
            records.sort(key=lambda x: x.get("publishTime", ""), reverse=True)
            total_duplicates += len(records) - 1
        
        deduplicated_data.append(records[0])
    
    logger.info(f"Deduplication completed: {len(deduplicated_data)} unique records, {total_duplicates} duplicates removed")
    return deduplicated_data

def handle_missing_fields(data: List[dict]) -> List[dict]:
    """Handle missing fields in wind & solar data."""
    if not data:
        return []
    
    logger.info(f"Processing missing fields for {len(data)} records")
    
    expected_fields = [
        "publishTime", "businessType", "psrType", "quantity",
        "startTime", "settlementDate", "settlementPeriod", "fuelType", "region"
    ]
    
    missing_stats = defaultdict(int)
    processed_data = []
    
    for record in data:
        processed_record = {}
        
        for field in expected_fields:
            if field in record and record[field] is not None:
                processed_record[field] = record[field]
            else:
                processed_record[field] = None
                missing_stats[field] += 1
        
        for field, value in record.items():
            if field not in expected_fields:
                processed_record[field] = value
        
        processed_data.append(processed_record)
    
    if missing_stats:
        logger.warning("Missing field statistics:")
        for field, count in missing_stats.items():
            percentage = (count / len(data)) * 100
            logger.warning(f"  {field}: {count} records ({percentage:.1f}%)")
    
    return processed_data

def validate_processed_data(data: List[dict]) -> Dict:
    """Validate processed wind & solar data quality."""
    if not data:
        return {"status": "error", "message": "No data to validate"}
    
    logger.info(f"Validating {len(data)} processed records")
    
    critical_fields = ["settlementDate", "settlementPeriod", "psrType", "quantity"]
    missing_critical = defaultdict(int)
    records_with_missing_critical = 0
    
    fuel_types = set()
    settlement_dates = set()
    quantities = []
    
    for record in data:
        has_missing_critical = False
        
        for field in critical_fields:
            if record.get(field) is None:
                missing_critical[field] += 1
                has_missing_critical = True
        
        if has_missing_critical:
            records_with_missing_critical += 1
        
        if record.get("psrType"):
            fuel_types.add(record["psrType"])
        if record.get("settlementDate"):
            settlement_dates.add(record["settlementDate"])
        if record.get("quantity") is not None:
            quantities.append(record["quantity"])
    
    data_quality_score = ((len(data) - records_with_missing_critical) / len(data)) * 100
    
    missing_stats = {}
    for field in critical_fields:
        if field in missing_critical:
            missing_stats[field] = {
                "count": missing_critical[field],
                "percentage": (missing_critical[field] / len(data)) * 100
            }
    
    quantity_stats = {}
    if quantities:
        quantity_stats = {
            "min": min(quantities),
            "max": max(quantities),
            "avg": sum(quantities) / len(quantities),
            "count": len(quantities)
        }
    
    validation_result = {
        "status": "success",
        "record_count": len(data),
        "fuel_types": sorted(list(fuel_types)),
        "date_range": {
            "min": min(settlement_dates) if settlement_dates else None,
            "max": max(settlement_dates) if settlement_dates else None,
            "unique_dates": len(settlement_dates)
        },
        "missing_stats": missing_stats,
        "data_quality_score": round(data_quality_score, 2),
        "quantity_stats": quantity_stats,
        "records_with_missing_critical": records_with_missing_critical
    }
    
    logger.info(f"Data validation completed: {data_quality_score:.2f}% quality score")
    return validation_result

