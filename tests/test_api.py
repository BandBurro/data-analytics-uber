import os
import csv
import tempfile
import pytest
from fastapi.testclient import TestClient

from src.api import main

# Colunas: Date, Time, Booking ID, Booking Status, Customer ID, Vehicle Type, Payment Method
SAMPLE_ROWS = [
    ["2025-09-01", "08:15:00", '"B001"', "Completed", '"C001"', "Sedan", "Card"],
    ["2025-09-01", "09:45:00", '"B002"', "Cancelled", '"C002"', "SUV", "Cash"],
    ["2025-09-02", "08:30:00", '"B003"', "Completed", '"C001"', "Sedan", "Card"],
    ["2025-09-03", "20:00:00", '"B004"', "Completed", '"C003"', "Bike", ""],
]

@pytest.fixture
def client_with_temp_csv():
    tmpdir = tempfile.TemporaryDirectory()
    csv_path = os.path.join(tmpdir.name, "cleaned_up_pandas.csv")

    # cria CSV temporário
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "Date", "Time", "Booking ID", "Booking Status", 
            "Customer ID", "Vehicle Type", "Payment Method"
        ])
        w.writerows(SAMPLE_ROWS)

    # redireciona a API para o CSV temporário e inicia o app
    main.CSV_PATH = csv_path
    # Force reload the data with the new CSV path
    main.load_df()
    client = TestClient(main.app)

    yield client

    client.close()
    tmpdir.cleanup()

def test_root_health_and_count(client_with_temp_csv):
    r = client_with_temp_csv.get("/")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["total_bookings"] == 4  # 4 linhas no CSV


def test_booking_status_breakdown(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/booking-status-breakdown")
    assert r.status_code == 200
    rows = r.json()
    # deve ter Completed (3) e Cancelled (1)
    counts = {row["booking_status"]: row["bookings"] for row in rows}
    assert counts["Completed"] == 3
    assert counts["Cancelled"] == 1

def test_bookings_per_hour(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/bookings-per-hour")
    assert r.status_code == 200
    rows = r.json()
    d = {row["hour"]: row["unique_bookings"] for row in rows}
    assert d[8] == 2
    assert d[9] == 1
    assert d[20] == 1

def test_bookings_per_weekday(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/bookings-per-weekday")
    assert r.status_code == 200
    rows = r.json()
    d = {row["weekday_num"]: row["unique_bookings"] for row in rows}
    assert d[1] == 2
    assert d[2] == 1
    assert d[3] == 1

def test_bookings_per_month(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/bookings-per-month")
    assert r.status_code == 200
    rows = r.json()
    assert any(row["month"] == "2025-09" and row["bookings"] == 4 for row in rows)

def test_peak_hours_with_limit(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/peak-hours", params={"limit": 2})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 2
    assert rows[0]["hour"] == 8
    assert rows[0]["unique_bookings"] == 2

def test_vehicle_types(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/vehicle-types")
    assert r.status_code == 200
    rows = r.json()
    d = {row["vehicle_type"]: (row["total_bookings"], row["unique_customers"]) for row in rows}
    assert d["Sedan"] == (2, 1)
    assert d["SUV"][0] == 1
    assert d["Bike"][0] == 1

def test_payment_methods(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/payment-methods")
    assert r.status_code == 200
    rows = r.json()
    d = {row["payment_method"]: row["total_bookings"] for row in rows}
    assert d["Card"] == 2
    assert d["Cash"] == 1

def test_top_customers_limit(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/top-customers", params={"limit": 2})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 2
    # top deve ser C001 com 2 bookings
    assert rows[0]["customer_id"] == "C001"
    assert rows[0]["total_bookings"] == 2

def test_top_customer_payment_methods(client_with_temp_csv):
    r = client_with_temp_csv.get("/analytics/top-customer-payment-methods")
    assert r.status_code == 200
    rows = r.json()
    # top customer é C001; método Card com 2 bookings
    assert any(
        row["customer_id"] == "C001" and row["payment_method"] == "Card" and row["bookings_for_method"] == 2
        for row in rows
    )

def test_bookings_list_default_limit(client_with_temp_csv):
    r = client_with_temp_csv.get("/bookings")
    assert r.status_code == 200
    data = r.json()
    assert data["total_found"] == 4
    assert data["returned"] == 4
    assert len(data["bookings"]) == 4

def test_bookings_filter_status_vehicle_customer(client_with_temp_csv):
    # status=Completed -> 3
    r1 = client_with_temp_csv.get("/bookings", params={"status": "Completed"})
    assert r1.status_code == 200
    assert r1.json()["total_found"] == 3

    # vehicle_type=Sedan -> 2
    r2 = client_with_temp_csv.get("/bookings", params={"vehicle_type": "Sedan"})
    assert r2.status_code == 200
    assert r2.json()["total_found"] == 2

    # customer_id=C001 -> 2 (a API envolve entre aspas internamente)
    r3 = client_with_temp_csv.get("/bookings", params={"customer_id": "C001"})
    assert r3.status_code == 200
    assert r3.json()["total_found"] == 2

def test_booking_by_id_found_and_not_found(client_with_temp_csv):
    ok = client_with_temp_csv.get("/bookings/B002")  # existe
    assert ok.status_code == 200
    miss = client_with_temp_csv.get("/bookings/NOPE")  # não existe
    assert miss.status_code == 404
