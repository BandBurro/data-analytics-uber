from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import os
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "uberda")
PG_USER = os.getenv("PG_USER", "root")
PG_PASSWORD = os.getenv("PG_PASSWORD", "practice")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine: sa.Engine = sa.create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

TABLE_NAME = "uber_bookings"

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

@asynccontextmanager
async def lifespan(app: FastAPI):
	try:
		with engine.connect() as conn:
			conn.execute(text("SELECT 1"))
	except Exception as exc:
		raise RuntimeError(f"Database connectivity failed: {exc}")
	yield

app = FastAPI(title="Uber Ride Analytics API (Postgres)", lifespan=lifespan)


def fetch_all(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
	with engine.connect() as conn:
		result = conn.execute(text(query), params or {})
		rows = [dict(r._mapping) for r in result]
		return rows

@app.get("/")
def root():
	try:
		row = fetch_all(f"SELECT COUNT(*) AS cnt FROM {TABLE_NAME}")[0]
		return {
			"status": "ok",
			"total_bookings": int(row["cnt"]),
			"description": "Uber Ride Analytics API - Backed by Postgres"
		}
	except Exception as e:
		raise HTTPException(500, f"Database error: {e}")

@app.get("/analytics/booking-status-breakdown", response_model=List[BookingStatusBreakdown])
def get_booking_status_breakdown():
	q = f"""
		SELECT booking_status,
		       COUNT(DISTINCT booking_id) AS bookings
		FROM {TABLE_NAME}
		GROUP BY booking_status
		ORDER BY bookings DESC
	"""
	return [BookingStatusBreakdown(**r) for r in fetch_all(q)]

@app.get("/analytics/bookings-per-hour", response_model=List[HourlyBookings])
def get_bookings_per_hour():
	q = f"""
		SELECT EXTRACT(HOUR FROM time)::INT AS hour,
		       COUNT(DISTINCT booking_id) AS unique_bookings
		FROM {TABLE_NAME}
		WHERE time IS NOT NULL
		GROUP BY hour
		ORDER BY hour
	"""
	return [HourlyBookings(**r) for r in fetch_all(q)]

@app.get("/analytics/bookings-per-weekday", response_model=List[WeeklyBookings])
def get_bookings_per_weekday():
	q = f"""
		SELECT EXTRACT(DOW FROM date)::INT AS weekday_num,
		       CASE EXTRACT(DOW FROM date)::INT
		         WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
		         WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
		         WHEN 6 THEN 'Saturday' END AS weekday_name,
		       COUNT(DISTINCT booking_id) AS unique_bookings
		FROM {TABLE_NAME}
		WHERE date IS NOT NULL
		GROUP BY weekday_num, weekday_name
		ORDER BY weekday_num
	"""
	return [WeeklyBookings(**r) for r in fetch_all(q)]

@app.get("/analytics/bookings-per-month", response_model=List[MonthlyBookings])
def get_bookings_per_month():
	q = f"""
		SELECT TO_CHAR(date, 'YYYY-MM') AS month,
		       COUNT(DISTINCT booking_id) AS bookings
		FROM {TABLE_NAME}
		WHERE date IS NOT NULL
		GROUP BY month
		ORDER BY month
	"""
	return [MonthlyBookings(**r) for r in fetch_all(q)]

@app.get("/analytics/peak-hours")
def get_peak_hours(limit: int = Query(5, ge=1, le=24)):
	q = f"""
		SELECT EXTRACT(HOUR FROM time)::INT AS hour,
		       COUNT(DISTINCT booking_id) AS unique_bookings
		FROM {TABLE_NAME}
		WHERE time IS NOT NULL
		GROUP BY hour
		ORDER BY unique_bookings DESC
		LIMIT :limit
	"""
	return fetch_all(q, {"limit": limit})

@app.get("/analytics/vehicle-types")
def get_vehicle_type_stats():
	q = f"""
		SELECT vehicle_type,
		       COUNT(DISTINCT booking_id) AS total_bookings,
		       COUNT(DISTINCT customer_id) AS unique_customers
		FROM {TABLE_NAME}
		GROUP BY vehicle_type
		ORDER BY total_bookings DESC
	"""
	return fetch_all(q)

@app.get("/analytics/payment-methods")
def get_payment_method_stats():
	q = f"""
		SELECT payment_method,
		       COUNT(DISTINCT booking_id) AS total_bookings
		FROM {TABLE_NAME}
		WHERE payment_method IS NOT NULL AND payment_method <> ''
		GROUP BY payment_method
		ORDER BY total_bookings DESC
	"""
	return fetch_all(q)

@app.get("/analytics/top-customers")
def get_top_customers(limit: int = Query(10, ge=1, le=100)):
	q = f"""
		SELECT customer_id,
		       COUNT(DISTINCT booking_id) AS total_bookings
		FROM {TABLE_NAME}
		GROUP BY customer_id
		ORDER BY total_bookings DESC
		LIMIT :limit
	"""
	return fetch_all(q, {"limit": limit})

@app.get("/analytics/top-customer-payment-methods", response_model=List[CustomerPaymentMethod])
def get_top_customer_payment_methods():
	q = f"""
		WITH top_customer AS (
			SELECT customer_id,
			       COUNT(DISTINCT booking_id) AS total_bookings
			FROM {TABLE_NAME}
			GROUP BY customer_id
			ORDER BY total_bookings DESC
			LIMIT 1
		)
		SELECT t.customer_id,
		       v.payment_method,
		       COUNT(DISTINCT v.booking_id) AS bookings_for_method
		FROM {TABLE_NAME} v
		JOIN top_customer t ON v.customer_id = t.customer_id
		GROUP BY t.customer_id, v.payment_method
		ORDER BY bookings_for_method DESC
	"""
	return [CustomerPaymentMethod(**r) for r in fetch_all(q)]

@app.get("/bookings")
def get_bookings(
	limit: int = Query(100, ge=1, le=1000),
	status: Optional[str] = None,
	vehicle_type: Optional[str] = None,
	customer_id: Optional[str] = None
):
	where_clauses = []
	params: Dict[str, Any] = {"limit": limit}
	if status:
		where_clauses.append("booking_status = :status")
		params["status"] = status
	if vehicle_type:
		where_clauses.append("vehicle_type = :vehicle_type")
		params["vehicle_type"] = vehicle_type
	if customer_id:
		where_clauses.append("customer_id = :customer_id")
		params["customer_id"] = customer_id

	where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

	q_count = f"SELECT COUNT(*) AS cnt FROM {TABLE_NAME} {where_sql}"
	total = fetch_all(q_count, params)[0]["cnt"]

	q_rows = f"""
		SELECT date, time, booking_id, booking_status, customer_id, vehicle_type, payment_method
		FROM {TABLE_NAME}
		{where_sql}
		ORDER BY date, time, booking_id
		LIMIT :limit
	"""
	rows = fetch_all(q_rows, params)
	return {"bookings": rows, "total_found": int(total), "returned": len(rows)}

@app.get("/bookings/{booking_id}")
def get_booking_by_id(booking_id: str):
	q = f"""
		SELECT date, time, booking_id, booking_status, customer_id, vehicle_type, payment_method
		FROM {TABLE_NAME}
		WHERE booking_id = :booking_id
		LIMIT 1
	"""
	rows = fetch_all(q, {"booking_id": booking_id})
	if not rows:
		raise HTTPException(404, "Booking not found")
	return rows[0]
