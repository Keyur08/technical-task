from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
import logging
from datetime import datetime

from api.dependencies import get_db_operations
from api.schemas import (
    FetchDataRequest, FetchDataResponse,
    RetrieveDataRequest, RetrieveDataResponse, 
    SummaryStats, DataRecord
)
from database.operations import DatabaseOperations
from utils.fetcher import fetch_generation_data, validate_data_quality
from utils.preprocessing import deduplicate_data, handle_missing_fields, validate_processed_data

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/fetch", response_model=FetchDataResponse)
async def fetch_and_store_data(
    request: FetchDataRequest,
    background_tasks: BackgroundTasks,
    db_ops: DatabaseOperations = Depends(get_db_operations)
):
    """
    Fetch data from API, process it, and store in database.
    Runs as background task for large date ranges.
    """
    start_time = datetime.utcnow()
    
    try:
        date_diff = (request.end_date - request.start_date).days + 1
        
        if date_diff > 30:
            background_tasks.add_task(
                _fetch_and_process_data,
                request.start_date.strftime("%Y-%m-%d"),
                request.end_date.strftime("%Y-%m-%d"),
                db_ops
            )
            
            return FetchDataResponse(
                status="processing",
                message=f"Background processing started for {date_diff} days",
                records_fetched=0,
                records_stored=0,
                processing_time=0.0,
                failed_chunks=0
            )
        
        result = await _fetch_and_process_data(
            request.start_date.strftime("%Y-%m-%d"),
            request.end_date.strftime("%Y-%m-%d"),
            db_ops
        )
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        return FetchDataResponse(
            status="completed",
            message="Data fetched and stored successfully",
            records_fetched=result["fetched"],
            records_stored=result["stored"],
            processing_time=processing_time,
            failed_chunks=result["failed_chunks"]
        )
        
    except Exception as e:
        logger.error(f"Error in fetch_and_store_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/retrieve", response_model=RetrieveDataResponse)
async def retrieve_data(
    request: RetrieveDataRequest,
    db_ops: DatabaseOperations = Depends(get_db_operations)
):
    """Retrieve stored data with optional filters."""
    try:
        # Convert fuel types to strings 
        fuel_types = [ft.value for ft in request.fuel_types] if request.fuel_types else None
        
        records = db_ops.get_data(
            start_date=request.start_date,
            end_date=request.end_date,
            fuel_types=fuel_types,
            limit=request.limit
        )
        
        # Convert to response format
        data = []
        for record in records:
            data.append(DataRecord(
                settlement_date=record.settlement_date,
                settlement_period=record.settlement_period,
                psr_type=record.psr_type,
                quantity=float(record.quantity) if record.quantity else 0.0,
                fuel_type=record.fuel_type or record.psr_type,
                region=record.region or "GB",
                publish_time=record.publish_time
            ))
        
        return RetrieveDataResponse(
            status="success",
            count=len(data),
            data=data
        )
        
    except Exception as e:
        logger.error(f"Error retrieving data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/summary", response_model=SummaryStats)
async def get_data_summary(db_ops: DatabaseOperations = Depends(get_db_operations)):
    """Get summary statistics of stored data."""
    try:
        summary = db_ops.get_summary_stats()
        
        return SummaryStats(
            total_records=summary["total_records"],
            unique_dates=summary.get("unique_dates", 0),
            fuel_type_breakdown=summary.get("fuel_type_breakdown", []),
            date_range=summary.get("date_range", {})
        )
        
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/clear")
async def clear_data(
    confirm: bool = False,
    db_ops: DatabaseOperations = Depends(get_db_operations)
):
    """Clear all data from database (use with caution)."""
    if not confirm:
        raise HTTPException(
            status_code=400, 
            detail="Must set confirm=true to clear data"
        )
    
    try:
        result = db_ops.clear_all_data()
        return {
            "status": "success",
            "message": f"Cleared {result['deleted_count']} records",
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error clearing data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _fetch_and_process_data(start_date: str, end_date: str, db_ops: DatabaseOperations) -> dict:
    """Internal function to fetch and process data."""
    try:
        # Step 1: Fetch raw data
        logger.info(f"Fetching data from {start_date} to {end_date}")
        raw_data = fetch_generation_data(start_date, end_date)
        
        if not raw_data:
            raise HTTPException(status_code=404, detail="No data found for the specified date range")
        
        # Step 2: Validate raw data
        validation_result = validate_data_quality(raw_data)
        if validation_result["status"] == "error":
            raise HTTPException(status_code=400, detail=f"Data validation failed: {validation_result['message']}")
        
        # Step 3: Process data
        logger.info("Processing data through pipeline")
        deduplicated_data = deduplicate_data(raw_data)
        processed_data = handle_missing_fields(deduplicated_data)
        
        # Step 4: Validate processed data
        final_validation = validate_processed_data(processed_data)
        if final_validation["status"] != "success":
            raise HTTPException(status_code=400, detail=f"Processed data validation failed: {final_validation['message']}")
        
        # Step 5: Store in database
        logger.info("Storing data in database")
        storage_result = db_ops.store_records(processed_data)
        
        return {
            "fetched": len(raw_data),
            "stored": storage_result["inserted"] + storage_result["updated"],
            "failed_chunks": 0  
        }
        
    except Exception as e:
        logger.error(f"Error in _fetch_and_process_data: {e}")
        raise