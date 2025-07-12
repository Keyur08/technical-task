from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
import logging
from typing import Generator
from config import settings
from .models import Base

logger = logging.getLogger(__name__)

class DatabaseConnection:
    _instance = None
    _engine = None
    _session_factory = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self, database_url: str = None):
        """Initialize database connection."""
        if self._engine is not None:
            return
            
        db_url = database_url or settings.database_url
        
        if "sqlite" in db_url:
            self._engine = create_engine(
                db_url,
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
                echo=False
            )
        else:
            self._engine = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                echo=False
            )
        
        self._session_factory = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self._engine
        )
        
        logger.info("Database connection initialized")
    
    def create_tables(self):
        """Create all tables."""
        if self._engine is None:
            raise RuntimeError("Database not initialized")
        
        Base.metadata.create_all(bind=self._engine)
        logger.info("Database tables created")
    
    def get_session(self) -> Session:
        """Get database session."""
        if self._session_factory is None:
            raise RuntimeError("Database not initialized")
        
        return self._session_factory()
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("Database connection closed")


db_connection = DatabaseConnection()

def initialize_database(database_url: str = None):
    """Initialize database connection and create tables."""
    db_connection.initialize(database_url)
    db_connection.create_tables()

def get_db_session() -> Generator[Session, None, None]:
    """Dependency to get database session."""
    session = db_connection.get_session()
    try:
        yield session
    finally:
        session.close()