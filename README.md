# Uber Ride Analytics

FastAPI APIs over Uber ride dataset with CSV (DuckDB) and Postgres backends.
## Analytics
### test
<img width="640" height="480" alt="booking_status_breakdown" src="https://github.com/user-attachments/assets/f5a9994b-85a1-4f59-89a4-20e9ae7daa07" />
test
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
