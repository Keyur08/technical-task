from typing import Generator
from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from database.connection import get_db_session
from database.operations import DatabaseOperations
from utils.visualization import DataVisualizer
import logging

logger = logging.getLogger(__name__)

def get_database() -> Session:
   """Database dependency."""
   db_gen = get_db_session()
   db = next(db_gen)
   try:
       return db
   except Exception as e:
       db.close()
       raise

def get_db_operations(db: Session = Depends(get_database)) -> DatabaseOperations:
   """Get database operations instance."""
   return DatabaseOperations(db)

def get_visualizer(db_ops: DatabaseOperations = Depends(get_db_operations)) -> DataVisualizer:
   """Get data visualizer instance."""
   return DataVisualizer(db_ops)