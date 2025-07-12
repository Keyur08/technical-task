from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
import tempfile
import logging
from datetime import datetime

from api.dependencies import get_visualizer
from api.schemas import GeneratePlotRequest, PlotResponse
from utils.visualization import DataVisualizer

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/generate")
async def generate_plot(
    request: GeneratePlotRequest,
    visualizer: DataVisualizer = Depends(get_visualizer)
):
    """
    Generate and return plot image.
    
    Plot Types:
    - daily: Daily generation over time (area chart)
    - monthly: Monthly comparison (bar chart)  
    - heatmap: Settlement period patterns (requires fuel_type)
    - fuel_comparison: Total generation by fuel type (bar + pie)
    """
    try:
        # Create temporary file for plot
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as temp_file:
            temp_path = temp_file.name
        
        # Convert fuel type to string if provided
        fuel_type = request.fuel_type if request.fuel_type else None
        
        # Generate plot based on type
        if request.plot_type == "daily":
            fig = visualizer.create_daily_generation_plot(
                start_date=request.start_date,
                end_date=request.end_date,
                save_path=temp_path,
                title=request.title
            )
        elif request.plot_type == "monthly":
            fig = visualizer.create_monthly_comparison_plot(
                start_date=request.start_date,
                end_date=request.end_date,
                save_path=temp_path,
                title=request.title
            )
        elif request.plot_type == "heatmap":
            fig = visualizer.create_settlement_period_heatmap(
                start_date=request.start_date,
                end_date=request.end_date,
                fuel_type=fuel_type,
                save_path=temp_path,
                title=request.title
            )
        elif request.plot_type == "fuel_comparison":
            fig = visualizer.create_fuel_comparison_plot(
                start_date=request.start_date,
                end_date=request.end_date,
                save_path=temp_path,
                title=request.title
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported plot type: {request.plot_type}")
        
        # Return file
        filename = f"{request.plot_type}_plot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        return FileResponse(
            temp_path,
            media_type="image/png",
            filename=filename,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error generating plot: {e}")
        raise HTTPException(status_code=500, detail=str(e))