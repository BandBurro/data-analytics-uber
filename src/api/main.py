from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import pandas as pd
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

CSV_PATH = os.getenv("CSV_PATH", "./data/cleaned_up_pandas.csv")
df_cache: Optional[pd.DataFrame] = None

class UberBooking(BaseModel):
    date: str
    time: str
    booking_id: str
    booking_status: str
    customer_id: str
    vehicle_type: str
    payment_method: Optional[str] = None

class BookingStatusBreakdown(BaseModel):
    booking_status: str
    bookings: int

class HourlyBookings(BaseModel):
    hour: int
    unique_bookings: int

class WeeklyBookings(BaseModel):
    weekday_num: int
    weekday_name: str
    unique_bookings: int

class MonthlyBookings(BaseModel):
    month: str
    bookings: int

class CustomerPaymentMethod(BaseModel):
    customer_id: str
    payment_method: str
    bookings_for_method: int

def load_df():
    global df_cache
    df_cache = pd.read_csv(CSV_PATH)

def save_df():
    if df_cache is not None:
        df_cache.to_csv(CSV_PATH, index=False)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    load_df()
    yield
    # shutdown
    save_df()

app = FastAPI(title="Uber Ride Analytics API", lifespan=lifespan)

def execute_duckdb_query(query: str) -> pd.DataFrame:
    """Execute a DuckDB query and return results as DataFrame"""
    conn = duckdb.connect()
    try:
        return conn.execute(query).df()
    finally:
        conn.close()

@app.get("/")
def root():
    rows = 0 if df_cache is None else len(df_cache)
    return {
        "status": "ok", 
        "total_bookings": rows,
        "description": "Uber Ride Analytics API - NCR Region Data"
    }

@app.get("/analytics/booking-status-breakdown", response_model=List[BookingStatusBreakdown])
def get_booking_status_breakdown():
    query = f"""
        SELECT "Booking Status" as booking_status,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        GROUP BY 1
        ORDER BY bookings DESC
    """
    result = execute_duckdb_query(query)
    return [BookingStatusBreakdown(**row) for row in result.to_dict('records')]

@app.get("/analytics/bookings-per-hour", response_model=List[HourlyBookings])
def get_bookings_per_hour():
    query = f"""
        SELECT EXTRACT(hour FROM Time) AS hour,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS unique_bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        WHERE Time IS NOT NULL
        GROUP BY 1
        ORDER BY hour
    """
    result = execute_duckdb_query(query)
    return [HourlyBookings(**row) for row in result.to_dict('records')]

@app.get("/analytics/bookings-per-weekday", response_model=List[WeeklyBookings])
def get_bookings_per_weekday():
    query = f"""
        SELECT EXTRACT(dow FROM Date) AS weekday_num,
               CASE EXTRACT(dow FROM Date)
                 WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
                 WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
                 WHEN 6 THEN 'Saturday' END AS weekday_name,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS unique_bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        WHERE Date IS NOT NULL
        GROUP BY 1,2
        ORDER BY weekday_num
    """
    result = execute_duckdb_query(query)
    return [WeeklyBookings(**row) for row in result.to_dict('records')]

@app.get("/analytics/bookings-per-month", response_model=List[MonthlyBookings])
def get_bookings_per_month():
    query = f"""
        SELECT strftime(Date, '%Y-%m') AS month,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        WHERE Date IS NOT NULL
        GROUP BY 1
        ORDER BY month
    """
    result = execute_duckdb_query(query)
    return [MonthlyBookings(**row) for row in result.to_dict('records')]

@app.get("/analytics/peak-hours")
def get_peak_hours(limit: int = Query(5, ge=1, le=24)):
    query = f"""
        SELECT EXTRACT(hour FROM Time) AS hour,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS unique_bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        WHERE Time IS NOT NULL
        GROUP BY 1
        ORDER BY unique_bookings DESC
        LIMIT {limit}
    """
    result = execute_duckdb_query(query)
    return result.to_dict('records')

@app.get("/analytics/vehicle-types")
def get_vehicle_type_stats():
    query = f"""
        SELECT "Vehicle Type" as vehicle_type,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS total_bookings,
               COUNT(DISTINCT REPLACE("Customer ID", '"', '')) AS unique_customers
        FROM read_csv_auto('{CSV_PATH}', header=True)
        GROUP BY 1
        ORDER BY total_bookings DESC
    """
    result = execute_duckdb_query(query)
    return result.to_dict('records')

@app.get("/analytics/payment-methods")
def get_payment_method_stats():
    query = f"""
        SELECT "Payment Method" as payment_method,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS total_bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        WHERE "Payment Method" IS NOT NULL AND "Payment Method" <> ''
        GROUP BY 1
        ORDER BY total_bookings DESC
    """
    result = execute_duckdb_query(query)
    return result.to_dict('records')

@app.get("/analytics/top-customers")
def get_top_customers(limit: int = Query(10, ge=1, le=100)):
    query = f"""
        SELECT REPLACE("Customer ID", '"', '') AS customer_id,
               COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS total_bookings
        FROM read_csv_auto('{CSV_PATH}', header=True)
        GROUP BY 1
        ORDER BY total_bookings DESC
        LIMIT {limit}
    """
    result = execute_duckdb_query(query)
    return result.to_dict('records')

@app.get("/analytics/top-customer-payment-methods", response_model=List[CustomerPaymentMethod])
def get_top_customer_payment_methods():
    query = f"""
        WITH top_customer AS (
          SELECT REPLACE("Customer ID", '"', '') AS customer_id,
                 COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS total_bookings
          FROM read_csv_auto('{CSV_PATH}', header=True)
          GROUP BY 1
          ORDER BY total_bookings DESC
          LIMIT 1
        )
        SELECT t.customer_id,
               v."Payment Method" as payment_method,
               COUNT(DISTINCT REPLACE(v."Booking ID", '"', '')) AS bookings_for_method
        FROM read_csv_auto('{CSV_PATH}', header=True) v
        JOIN top_customer t
          ON REPLACE(v."Customer ID", '"', '') = t.customer_id
        GROUP BY t.customer_id, v."Payment Method"
        ORDER BY bookings_for_method DESC
    """
    result = execute_duckdb_query(query)
    return [CustomerPaymentMethod(**row) for row in result.to_dict('records')]

@app.get("/bookings")
def get_bookings(
    limit: int = Query(100, ge=1, le=1000),
    status: Optional[str] = None,
    vehicle_type: Optional[str] = None,
    customer_id: Optional[str] = None
):
    if df_cache is None:
        raise HTTPException(500, "Data not loaded")
    
    filtered_df = df_cache.copy()
    
    if status:
        filtered_df = filtered_df[filtered_df["Booking Status"] == status]
    if vehicle_type:
        filtered_df = filtered_df[filtered_df["Vehicle Type"] == vehicle_type]
    if customer_id:
        clean_customer_id = f'"{customer_id}"'
        filtered_df = filtered_df[filtered_df["Customer ID"] == clean_customer_id]
    
    result = filtered_df.head(limit)
    # Handle NaN values for JSON serialization
    bookings_data = result.to_dict(orient="records")
    for booking in bookings_data:
        for key, value in booking.items():
            if pd.isna(value):
                booking[key] = None
    
    return {
        "bookings": bookings_data,
        "total_found": len(filtered_df),
        "returned": len(result)
    }

@app.get("/bookings/{booking_id}")
def get_booking_by_id(booking_id: str):
    if df_cache is None:
        raise HTTPException(500, "Data not loaded")
    
    # Clean the booking_id to match the format in the data
    clean_booking_id = f'"{booking_id}"'
    booking = df_cache[df_cache["Booking ID"] == clean_booking_id]
    
    if booking.empty:
        raise HTTPException(404, "Booking not found")
    
    booking_data = booking.to_dict(orient="records")[0]
    # Handle NaN values for JSON serialization
    for key, value in booking_data.items():
        if pd.isna(value):
            booking_data[key] = None
    
    return booking_data
