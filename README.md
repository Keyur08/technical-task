Wind & Solar Data Pipeline API


Step-1
pip install -r requirements.txt


Step-2
uvicorn main:app --reload --host 0.0.0.0 --port 8000


Step-3
Access Documentation
Interactive API Docs: http://localhost:8000/docs

## Plot Types

| Type             | Description                     | Required Parameters                  |
|------------------|---------------------------------|---------------------------------------|
| `daily`          | Daily generation time series    | `start_date`, `end_date`             |
| `monthly`        | Monthly comparison bars         | `start_date`, `end_date`             |
| `heatmap`        | Settlement period patterns      | `fuel_type`, `date_range`            |
| `fuel_comparison`| Fuel type totals                | `date_range`                         |



### Testing
#to test whole pipeline at once.
pytest tests/test_complete_pipeline.py -v -s


Requirements

Python 3.8+
PostgreSQL 
Dependencies in requirements.txt