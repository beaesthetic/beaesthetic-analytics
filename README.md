# BeAesthetic Analytics Service

Analytics service for appointment data with Month-over-Month (MoM) and Year-over-Year (YoY) comparisons.

## Setup

```bash
uv sync
```

## Run

```bash
uv run uvicorn analytics.main:app --reload
```

## API Endpoints

- `GET /health` - Health check
- `GET /appointments?start_date=...&end_date=...` - Get appointments in date range
- `GET /appointments/daily?start_date=...&end_date=...` - Daily breakdown
- `GET /appointments/mom?year=2024&month=1` - Month over Month comparison
- `GET /appointments/yoy?year=2024&month=1` - Year over Year comparison

## Configuration

Set environment variables:

- `ANALYTICS_MONGODB_URI` - MongoDB connection string (default: `mongodb://localhost:27017`)
- `ANALYTICS_MONGODB_DATABASE` - Database name (default: `beaesthetic`)
