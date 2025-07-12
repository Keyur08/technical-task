import logging
import uvicorn
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from config import settings
from database.connection import initialize_database
from api.routes.health import router as health_router
from api.routes.data import router as data_router
from api.routes.plots import router as plots_router

#logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Wind & Solar Data Pipeline API")
    try:
        initialize_database()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Wind & Solar Data Pipeline API")

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description="API for fetching, processing, and visualizing wind & solar generation data",
    lifespan=lifespan
)

# middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#API routes
router = APIRouter()
router.include_router(health_router, tags=["health"])
router.include_router(data_router, prefix="/data", tags=["data"])
router.include_router(plots_router, prefix="/plots", tags=["plots"])

app.include_router(router, prefix="/api/v1")

@app.get("/")
async def root():
    return {
        "message": "Wind & Solar Data Pipeline API",
        "version": settings.app_version,
        "docs": "/docs"
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
        log_level=settings.log_level.lower()
    )