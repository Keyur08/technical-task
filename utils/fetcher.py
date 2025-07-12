# utils/fetcher.py
import httpx
import logging
import time
from datetime import datetime, timedelta
from typing import Generator, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type/wind-and-solar"
HEADERS = {"accept": "application/json"}
SETTLEMENT_PERIOD_FROM = 1
SETTLEMENT_PERIOD_TO = 50
FORMAT = "json"
MAX_CHUNK_DAYS = 6
REQUEST_TIMEOUT = 60
RATE_LIMIT_DELAY = 1

def date_chunks(start: datetime, end: datetime, days: int = MAX_CHUNK_DAYS) -> Generator[tuple[datetime, datetime], None, None]:
    """Generate date chunks for API requests."""
    current = start
    chunk_count = 0
    
    while current <= end:
        chunk_end = current + timedelta(days=days - 1)
        chunk_end = min(chunk_end, end)
        
        actual_days = (chunk_end - current).days + 1
        if actual_days > days:
            chunk_end = current + timedelta(days=days - 1)
            
        chunk_count += 1
        logger.info(f"Generated chunk {chunk_count}: {current.date()} to {chunk_end.date()} ({actual_days} days)")
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
)
def fetch_single_chunk(from_dt: datetime, to_dt: datetime) -> Optional[List[dict]]:
    """Fetch data for a single date chunk with retry logic."""
    params = {
        "from": from_dt.strftime("%Y-%m-%d"),
        "to": to_dt.strftime("%Y-%m-%d"),
        "settlementPeriodFrom": SETTLEMENT_PERIOD_FROM,
        "settlementPeriodTo": SETTLEMENT_PERIOD_TO,
        "format": FORMAT
    }
    
    days_diff = (to_dt - from_dt).days + 1
    if days_diff > 7:
        logger.error(f"Date range too large: {days_diff} days")
        raise ValueError(f"Date range exceeds 7 days: {days_diff} days")
    
    logger.info(f"Fetching data from {from_dt.date()} to {to_dt.date()} ({days_diff} days)")
    
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            response = client.get(BASE_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            result = response.json()
            raw_data = result.get("data", [])
            
            transformed_data = []
            for record in raw_data:
                transformed_record = {
                    "publishTime": record.get("publishTime"),
                    "businessType": record.get("businessType"),
                    "psrType": record.get("psrType"),
                    "fuelType": record.get("psrType"),
                    "quantity": record.get("quantity"),
                    "startTime": record.get("startTime"),
                    "settlementDate": record.get("settlementDate"),
                    "settlementPeriod": record.get("settlementPeriod"),
                    "region": "GB"
                }
                transformed_data.append(transformed_record)
            
            logger.info(f"Successfully fetched {len(transformed_data)} records")
            return transformed_data
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error {e.response.status_code}: {e}")
        raise
    except httpx.RequestError as e:
        logger.error(f"Request error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def fetch_generation_data(start_date: str, end_date: str) -> List[dict]:
    """Fetch generation data for the specified date range."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        logger.info(f"Starting data fetch for period: {start_date} to {end_date}")
        
        if start > end:
            raise ValueError(f"Start date {start_date} is after end date {end_date}")
        
        total_days = (end - start).days + 1
        expected_chunks = (total_days + MAX_CHUNK_DAYS - 1) // MAX_CHUNK_DAYS
        
        logger.info(f"Total days: {total_days}, Expected chunks: {expected_chunks}")
        
        all_data = []
        successful_chunks = 0
        failed_chunks = 0
        
        for chunk_num, (from_dt, to_dt) in enumerate(date_chunks(start, end), 1):
            try:
                logger.info(f"Processing chunk {chunk_num}/{expected_chunks}")
                
                data = fetch_single_chunk(from_dt, to_dt)
                
                if data:
                    all_data.extend(data)
                    successful_chunks += 1
                    logger.info(f"Chunk {chunk_num} successful. Total records: {len(all_data)}")
                else:
                    logger.warning(f"Chunk {chunk_num} returned no data")
                
                if chunk_num < expected_chunks:
                    time.sleep(RATE_LIMIT_DELAY)
                    
            except Exception as e:
                failed_chunks += 1
                logger.error(f"Chunk {chunk_num} failed: {e}")
                continue
        
        logger.info(f"Data fetch completed. Total records: {len(all_data)}")
        logger.info(f"Successful chunks: {successful_chunks}, Failed chunks: {failed_chunks}")
        
        return all_data
        
    except ValueError as e:
        logger.error(f"Invalid date format or range: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def validate_data_quality(data: List[dict]) -> dict:
    """Validate the quality of fetched data."""
    if not data:
        return {"status": "error", "message": "No data received"}
    
    total_records = len(data)
    required_fields = ["settlementDate", "settlementPeriod", "psrType", "quantity"]
    missing_fields = []
    
    sample_size = min(100, len(data))
    for record in data[:sample_size]:
        for field in required_fields:
            if field not in record or record[field] is None:
                missing_fields.append(field)
    
    fuel_types = set(record.get("psrType", "") for record in data if record.get("psrType"))
    dates = [record.get("settlementDate", "") for record in data if record.get("settlementDate")]
    min_date = min(dates) if dates else None
    max_date = max(dates) if dates else None
    
    return {
        "status": "success",
        "total_records": total_records,
        "fuel_types": list(fuel_types),
        "date_range": {"min": min_date, "max": max_date},
        "missing_fields": list(set(missing_fields))
    }

def get_failed_date_ranges(start_date: str, end_date: str, successful_data: List[dict]) -> List[tuple[str, str]]:
    """Identify date ranges that failed to fetch."""
    if not successful_data:
        return [(start_date, end_date)]
    
    successful_dates = set()
    for record in successful_data:
        if record.get("settlementDate"):
            successful_dates.add(record["settlementDate"])
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    failed_ranges = []
    current = start
    
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in successful_dates:
            range_end = current
            while range_end <= end:
                next_date = range_end + timedelta(days=1)
                if next_date > end or next_date.strftime("%Y-%m-%d") in successful_dates:
                    break
                range_end = next_date
            
            failed_ranges.append((date_str, range_end.strftime("%Y-%m-%d")))
            current = range_end + timedelta(days=1)
        else:
            current += timedelta(days=1)
    
    return failed_ranges
