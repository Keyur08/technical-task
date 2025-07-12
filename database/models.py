from sqlalchemy import Column, Integer, String, DateTime, Date, Numeric, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class WindSolarGeneration(Base):
    __tablename__ = "wind_solar_generation"
    
    id = Column(Integer, primary_key=True, index=True)
    publish_time = Column(DateTime(timezone=True))
    business_type = Column(String(50))
    psr_type = Column(String(50), index=True)
    quantity = Column(Numeric(10, 3))
    start_time = Column(DateTime(timezone=True))
    settlement_date = Column(Date, index=True)
    settlement_period = Column(Integer, index=True)
    fuel_type = Column(String(50), index=True)
    region = Column(String(10))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        UniqueConstraint('settlement_date', 'settlement_period', 'psr_type', name='unique_generation_record'),
        Index('idx_composite_query', 'settlement_date', 'psr_type', 'settlement_period'),
    )

    def __repr__(self):
        return f"<WindSolarGeneration(id={self.id}, date={self.settlement_date}, period={self.settlement_period}, type={self.psr_type})>"
