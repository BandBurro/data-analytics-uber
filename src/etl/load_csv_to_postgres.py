import os
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, Integer, String, Date, Time
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dotenv import load_dotenv


def get_engine_from_env() -> Engine:
    load_dotenv()
    pg_host = os.getenv("PG_HOST", "127.0.0.1")
    pg_port = int(os.getenv("PG_PORT", "5432"))
    pg_db = os.getenv("PG_DB", "uberda")
    pg_user = os.getenv("PG_USER", "root")
    pg_password = os.getenv("PG_PASSWORD", "practice")

    url = (
        f"postgresql+psycopg2://{pg_user}:{pg_password}"
        f"@{pg_host}:{pg_port}/{pg_db}"
    )
    return sa.create_engine(url, echo=False)


def define_schema(metadata: MetaData) -> Table:
    return Table(
        "uber_bookings",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("date", Date, nullable=False),
        Column("time", Time, nullable=False),
        Column("booking_id", String(50), nullable=False, unique=True),
        Column("booking_status", String(50), nullable=False),
        Column("customer_id", String(50), nullable=False),
        Column("vehicle_type", String(50), nullable=False),
        Column("payment_method", String(50), nullable=True),
    )


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Booking ID" in df.columns:
        df["Booking ID"] = df["Booking ID"].astype(str).str.replace('"', "", regex=False)
    if "Customer ID" in df.columns:
        df["Customer ID"] = df["Customer ID"].astype(str).str.replace('"', "", regex=False)

    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df["Time"] = pd.to_datetime(df["Time"], format="%H:%M:%S", errors="coerce").dt.time
    df["Payment Method"] = df.get("Payment Method", "").fillna("")

    df = df.rename(
        columns={
            "Date": "date",
            "Time": "time",
            "Booking ID": "booking_id",
            "Booking Status": "booking_status",
            "Customer ID": "customer_id",
            "Vehicle Type": "vehicle_type",
            "Payment Method": "payment_method",
        }
    )
    return df[
        [
            "date",
            "time",
            "booking_id",
            "booking_status",
            "customer_id",
            "vehicle_type",
            "payment_method",
        ]
    ]


def chunk_iterable(items: List[Dict], size: int) -> Iterable[List[Dict]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def upsert_rows(engine: Engine, table: Table, rows: List[Dict], batch_size: int = 1000) -> int:
    inserted = 0
    with engine.begin() as conn:
        for batch in chunk_iterable(rows, batch_size):
            stmt = pg_insert(table).values(batch)
            stmt = stmt.on_conflict_do_nothing(index_elements=[table.c.booking_id])
            res = conn.execute(stmt)
            inserted += res.rowcount or 0
    return inserted


def create_indexes(engine: Engine) -> None:
    index_sql = [
        "CREATE INDEX IF NOT EXISTS idx_booking_date ON uber_bookings(date)",
        "CREATE INDEX IF NOT EXISTS idx_booking_status ON uber_bookings(booking_status)",
        "CREATE INDEX IF NOT EXISTS idx_vehicle_type ON uber_bookings(vehicle_type)",
        "CREATE INDEX IF NOT EXISTS idx_customer_id ON uber_bookings(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_payment_method ON uber_bookings(payment_method)",
        "CREATE INDEX IF NOT EXISTS idx_date_time ON uber_bookings(date, time)",
    ]
    with engine.begin() as conn:
        for sql in index_sql:
            conn.execute(sa.text(sql))


def main() -> None:
    load_dotenv()
    csv_path = os.getenv("CSV_PATH", "./data/cleaned_up_pandas.csv")
    csv_path = str(Path(csv_path))

    engine = get_engine_from_env()

    metadata = MetaData()
    uber_bookings = define_schema(metadata)

    metadata.create_all(engine)

    df = pd.read_csv(csv_path)
    df = normalize_dataframe(df)

    rows = df.to_dict("records")
    inserted = upsert_rows(engine, uber_bookings, rows, batch_size=1000)

    create_indexes(engine)

    print(f"Inserted rows: {inserted}")


if __name__ == "__main__":
    main()


