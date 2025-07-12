from pydantic import BaseModel, Field, validator
from typing import Optional, List, Literal
from datetime import date, datetime
from enum import Enum
from pydantic import validator


class FuelType(str, Enum):
    WIND_OFFSHORE = "Wind Offshore"
    WIND_ONSHORE = "Wind Onshore" 
    SOLAR = "Solar"

class PlotType(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly" 
    HEATMAP = "heatmap"
    FUEL_COMPARISON = "fuel_comparison"

class FetchDataRequest(BaseModel):
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError('End date must be after start date')
        return v
    
    @validator('start_date', 'end_date')
    def validate_date_not_future(cls, v):
        if v > date.today():
            raise ValueError('Date cannot be in the future')
        return v

class RetrieveDataRequest(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    fuel_types: Optional[List[FuelType]] = None
    limit: Optional[int] = Field(None, ge=1, le=10000)
    
class GeneratePlotRequest(BaseModel):
    plot_type: PlotType
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    fuel_type: Optional[str] = None 
    title: Optional[str] = None
    
    @validator('fuel_type')
    def normalize_fuel_type(cls, v):
        if v is None:
            return v
        return v.strip().lower()

class DataRecord(BaseModel):
    settlement_date: date
    settlement_period: int
    psr_type: str
    quantity: float
    fuel_type: str
    region: str
    publish_time: Optional[datetime] = None

class FetchDataResponse(BaseModel):
    status: str
    message: str
    records_fetched: int
    records_stored: int
    processing_time: float
    failed_chunks: int

class RetrieveDataResponse(BaseModel):
    status: str
    count: int
    data: List[DataRecord]

class PlotResponse(BaseModel):
    status: str
    plot_type: str
    filename: str
    message: str

class SummaryStats(BaseModel):
    total_records: int
    unique_dates: int
    fuel_type_breakdown: List[dict]
    date_range: dict