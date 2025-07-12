from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc
from typing import List, Dict, Optional, Tuple
from datetime import date, datetime
import logging
from .models import WindSolarGeneration

logger = logging.getLogger(__name__)

class DatabaseOperations:
    def __init__(self, session: Session):
        self.session = session
    
    def store_records(self, records: List[Dict]) -> Dict:
        """Store records with upsert logic."""
        if not records:
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        try:
            for record in records:
                try:
                    # Parse dates if they're strings
                    settlement_date = record.get('settlementDate')
                    if isinstance(settlement_date, str):
                        settlement_date = datetime.strptime(settlement_date, '%Y-%m-%d').date()
                    
                    publish_time = record.get('publishTime')
                    if isinstance(publish_time, str) and publish_time:
                        publish_time = datetime.fromisoformat(publish_time.replace('Z', '+00:00'))
                    
                    start_time = record.get('startTime')
                    if isinstance(start_time, str) and start_time:
                        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    
                    # Check if record exists
                    existing = self.session.query(WindSolarGeneration).filter(
                        and_(
                            WindSolarGeneration.settlement_date == settlement_date,
                            WindSolarGeneration.settlement_period == record.get('settlementPeriod'),
                            WindSolarGeneration.psr_type == record.get('psrType')
                        )
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.publish_time = publish_time
                        existing.business_type = record.get('businessType')
                        existing.quantity = record.get('quantity')
                        existing.start_time = start_time
                        existing.fuel_type = record.get('fuelType')
                        existing.region = record.get('region', 'GB')
                        existing.updated_at = datetime.utcnow()
                        updated_count += 1
                    else:
                        # Insert new record
                        new_record = WindSolarGeneration(
                            publish_time=publish_time,
                            business_type=record.get('businessType'),
                            psr_type=record.get('psrType'),
                            quantity=record.get('quantity'),
                            start_time=start_time,
                            settlement_date=settlement_date,
                            settlement_period=record.get('settlementPeriod'),
                            fuel_type=record.get('fuelType'),
                            region=record.get('region', 'GB')
                        )
                        self.session.add(new_record)
                        inserted_count += 1
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing record: {e}")
                    continue
            
            self.session.commit()
            
            logger.info(f"Database operation completed: {inserted_count} inserted, {updated_count} updated, {error_count} errors")
            
            return {
                "inserted": inserted_count,
                "updated": updated_count,
                "errors": error_count
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
    
    def get_data(self, 
            start_date: Optional[date] = None,
            end_date: Optional[date] = None,
            fuel_types: Optional[List[str]] = None,
            limit: Optional[int] = None) -> List[WindSolarGeneration]:
        """Retrieve data with filters."""
        query = self.session.query(WindSolarGeneration)
        
        if start_date:
            query = query.filter(WindSolarGeneration.settlement_date >= start_date)
        if end_date:
            query = query.filter(WindSolarGeneration.settlement_date <= end_date)
        if fuel_types:
            lower_fuel_types = [ft.lower() for ft in fuel_types]
            query = query.filter(func.lower(WindSolarGeneration.psr_type).in_(lower_fuel_types))
        
        query = query.order_by(
            WindSolarGeneration.settlement_date,
            WindSolarGeneration.settlement_period
        )
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_summary_stats(self) -> Dict:
        """Get comprehensive summary statistics."""
        try:
            # Basic stats
            total_records = self.session.query(func.count(WindSolarGeneration.id)).scalar() or 0
            
            if total_records == 0:
                return {
                    "total_records": 0,
                    "unique_dates": 0,
                    "fuel_type_breakdown": [],
                    "date_range": {"min": None, "max": None}
                }
            
            # Date range
            date_range = self.session.query(
                func.min(WindSolarGeneration.settlement_date),
                func.max(WindSolarGeneration.settlement_date)
            ).first()
            
            # Unique dates count
            unique_dates = self.session.query(
                func.count(func.distinct(WindSolarGeneration.settlement_date))
            ).scalar() or 0
            
            # Fuel type breakdown
            fuel_stats = self.session.query(
                WindSolarGeneration.psr_type,
                func.count(WindSolarGeneration.id).label('count'),
                func.avg(WindSolarGeneration.quantity).label('avg_quantity'),
                func.sum(WindSolarGeneration.quantity).label('total_quantity'),
                func.min(WindSolarGeneration.settlement_date).label('min_date'),
                func.max(WindSolarGeneration.settlement_date).label('max_date')
            ).group_by(WindSolarGeneration.psr_type).all()
            
            fuel_breakdown = []
            for stat in fuel_stats:
                fuel_breakdown.append({
                    "fuel_type": stat.psr_type,
                    "count": stat.count,
                    "avg_quantity": float(stat.avg_quantity) if stat.avg_quantity else 0,
                    "total_quantity": float(stat.total_quantity) if stat.total_quantity else 0,
                    "min_date": str(stat.min_date) if stat.min_date else None,
                    "max_date": str(stat.max_date) if stat.max_date else None
                })
            
            return {
                "total_records": total_records,
                "unique_dates": unique_dates,
                "fuel_type_breakdown": fuel_breakdown,
                "date_range": {
                    "min": str(date_range[0]) if date_range[0] else None,
                    "max": str(date_range[1]) if date_range[1] else None
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting summary stats: {e}")
            raise
    
    def get_latest_record(self) -> Optional[WindSolarGeneration]:
        """Get the most recent record."""
        return self.session.query(WindSolarGeneration).order_by(
            desc(WindSolarGeneration.settlement_date),
            desc(WindSolarGeneration.settlement_period)
        ).first()
    
    def get_data_by_date_range(self, start_date: date, end_date: date) -> List[WindSolarGeneration]:
        """Get all data within a date range."""
        return self.session.query(WindSolarGeneration).filter(
            and_(
                WindSolarGeneration.settlement_date >= start_date,
                WindSolarGeneration.settlement_date <= end_date
            )
        ).order_by(
            WindSolarGeneration.settlement_date,
            WindSolarGeneration.settlement_period
        ).all()
    
    def get_fuel_type_data(self, fuel_type: str, limit: Optional[int] = None) -> List[WindSolarGeneration]:
        """Get data for specific fuel type."""
        query = self.session.query(WindSolarGeneration).filter(
            WindSolarGeneration.psr_type == fuel_type
        ).order_by(
            desc(WindSolarGeneration.settlement_date),
            desc(WindSolarGeneration.settlement_period)
        )
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_daily_totals(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[Tuple]:
        """Get daily generation totals by fuel type."""
        query = self.session.query(
            WindSolarGeneration.settlement_date,
            WindSolarGeneration.psr_type,
            func.sum(WindSolarGeneration.quantity).label('daily_total')
        )
        
        if start_date:
            query = query.filter(WindSolarGeneration.settlement_date >= start_date)
        if end_date:
            query = query.filter(WindSolarGeneration.settlement_date <= end_date)
        
        return query.group_by(
            WindSolarGeneration.settlement_date,
            WindSolarGeneration.psr_type
        ).order_by(WindSolarGeneration.settlement_date).all()
    
    def clear_all_data(self) -> Dict:
        """Clear all data from the table."""
        try:
            deleted_count = self.session.query(WindSolarGeneration).count()
            self.session.query(WindSolarGeneration).delete()
            self.session.commit()
            
            logger.info(f"Cleared {deleted_count} records from database")
            
            return {"deleted_count": deleted_count}
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error clearing data: {e}")
            raise
    
    def health_check(self) -> Dict:
        """Check database health."""
        try:
            count = self.session.query(func.count(WindSolarGeneration.id)).scalar()
            
            return {
                "status": "healthy",
                "total_records": count,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow()
            }