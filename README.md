# Uber Ride Analytics

A comprehensive data analytics project exploring Uber ride patterns with FastAPI APIs, ETL pipelines, and interactive visualizations. Features both CSV (DuckDB/Pandas) and Postgres backends for flexible data analysis.

## Analytics & Insights

This project provides deep insights into Uber ride patterns through comprehensive data analysis and visualizations:

### Key Metrics Analyzed
- **150,000+ ride bookings** across different vehicle types
- **Time-based patterns**: hourly, daily, and monthly booking trends
- **Customer behavior**: payment preferences and booking frequency
- **Service performance**: completion rates and cancellation patterns
- **Vehicle type preferences**: demand distribution across ride options

### Visualizations

#### Booking Patterns Over Time
![Bookings per Hour](src/viz/bookings_per_hour.png)
*Peak booking hours and daily usage patterns*

![Bookings per Weekday](src/viz/bookings_per_weekday.png)
*Weekly booking distribution showing weekend vs weekday preferences*

![Bookings per Month](src/viz/bookings_per_month.png)
*Monthly trends and seasonal patterns*

#### Vehicle Type Analysis
![Bookings by Vehicle Type](src/viz/bookings_by_vehicle_type.png)
*Popularity distribution across different Uber vehicle categories*

![Payment Methods by Vehicle Type](src/viz/payment_methods_by_vehicle_type.png)
*Payment preference correlation with vehicle type selection*

#### Service Performance
![Booking Status Breakdown](src/viz/booking_status_breakdown.png)
*Service completion rates and cancellation analysis*

![Top Customer Payment Methods](src/viz/top_customer_payment_methods.png)
*Payment method preferences of frequent customers*

### Data Schema
The analysis is built on a comprehensive dataset with the following structure:
- **Date/Time**: Temporal analysis capabilities
- **Booking ID**: Unique identifier for each ride
- **Customer ID**: Customer behavior tracking
- **Vehicle Type**: Service category analysis
- **Payment Method**: Transaction preference insights
- **Booking Status**: Service performance metrics

## Setup

1) Create/activate venv
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2) Install deps
```bash
pip install -r requirements.txt
```

3) Environment
- Copy `ENV.sample` to `.env` and adjust values
```bash
cp ENV.sample .env
```
- CSV-backed API uses `CSV_PATH` (default `./data/cleaned_up_pandas.csv`)
- Postgres API uses `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`

### One-liners (Makefile)
```bash
make env install        # set up .env and install deps
make run-csv            # start CSV-backed API at :8000
make run-pg             # start Postgres API at :8001
make etl                # load CSV into Postgres (uses CSV_PATH)
make test               # run tests
```

## Run

CSV-backed API:
```bash
uvicorn src.api.main:app --reload --port 8000
```

Postgres-backed API:
```bash
uvicorn src.api.main_db:app --reload --port 8001
```

## ETL: Load CSV into Postgres

Configure `.env` (or `ENV.sample`) with Postgres credentials and `CSV_PATH` to the CSV file (defaults to `./data/cleaned_up_pandas.csv`), then run:

```bash
make etl
# or
python -m src.etl.load_csv_to_postgres
```

This will:
- Create the `uber_bookings` table (id, date, time, booking_id unique, booking_status, customer_id, vehicle_type, payment_method)
- Normalize the CSV schema
- Upsert with `ON CONFLICT DO NOTHING` on `booking_id`
- Create helpful indexes

## Tests
```bash
python -m pytest -q
```

## Notes
- `pytest.ini` sets `pythonpath=.` so imports like `from src.api import main` work.
- Large data files in `data/` are git-ignored; place your CSVs there.
