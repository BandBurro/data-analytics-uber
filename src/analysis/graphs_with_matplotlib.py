import duckdb as ddb
import pandas as pd
import matplotlib.pyplot as plt
import os

# save graphs into the src/viz directory (the directory of this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = SCRIPT_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)

DATA_FILE = os.path.join(SCRIPT_DIR, "..", "..", "data", "cleaned_up_pandas.csv")
df = pd.read_csv(DATA_FILE)

con = ddb.connect()

# create view on CSV (duckdb will infer types)
con.execute(f"""
  CREATE OR REPLACE VIEW v AS
  SELECT * FROM read_csv_auto('{DATA_FILE}', header=True)
""")

con.register("v", df)

queries = {
  "bookings_per_hour": """
    SELECT date_part('hour', CAST(Time AS TIME)) AS hour,
           COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS unique_bookings
    FROM v
    WHERE Time IS NOT NULL AND Time <> ''
    GROUP BY 1
    ORDER BY hour;
  """,

  "bookings_per_weekday": """
    SELECT CAST(strftime(CAST(Date AS DATE), '%w') AS INTEGER) AS weekday_num,
           CASE strftime(CAST(Date AS DATE), '%w')
             WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday'
             WHEN '3' THEN 'Wednesday' WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday'
             WHEN '6' THEN 'Saturday' END AS weekday_name,
           COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS unique_bookings
    FROM v
    WHERE Date IS NOT NULL AND Date <> ''
    GROUP BY 1,2
    ORDER BY weekday_num;
  """,

  "bookings_per_month": """
    SELECT strftime(CAST(Date AS DATE), '%Y-%m') AS month,
           COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS bookings
    FROM v
    WHERE Date IS NOT NULL AND Date <> ''
    GROUP BY 1
    ORDER BY month;
  """,

  "booking_status_breakdown": """
    SELECT "Booking Status",
           COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS bookings
    FROM v
    GROUP BY 1
    ORDER BY bookings DESC;
  """,

  "top_customer_and_payment_methods": """
    WITH top_customer AS (
      SELECT REPLACE("Customer ID", '"', '') AS customer_id,
             COUNT(DISTINCT REPLACE("Booking ID", '"', '')) AS total_bookings
      FROM v
      GROUP BY 1
      ORDER BY total_bookings DESC
      LIMIT 1
    )
    SELECT t.customer_id,
           v."Payment Method" AS payment_method,
           COUNT(DISTINCT REPLACE(v."Booking ID", '"', '')) AS bookings_for_method
    FROM v
    JOIN top_customer t
      ON REPLACE(v."Customer ID", '"', '') = t.customer_id
    GROUP BY t.customer_id, v."Payment Method"
    ORDER BY bookings_for_method DESC;
  """
}

def plot_bar(df, x, y, title, xlabel=None, ylabel=None, rotate=0, highlight_idx=None, out_name=None):
    plt.figure(figsize=(10, 6))
    colors = ['#1f77b4'] * len(df)
    if highlight_idx is not None and 0 <= highlight_idx < len(df):
        colors[highlight_idx] = '#d62728'
    ax = df.plot(kind="bar", x=x, y=y, legend=False, color=colors, rot=rotate)
    ax.set_title(title)
    if xlabel: ax.set_xlabel(xlabel)
    if ylabel: ax.set_ylabel(ylabel)
    plt.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, out_name)
    plt.savefig(out_path)
    plt.close()
    print("Generated:", out_path)

# 1) bookings per hour + hour with most bookings
df_hour = con.execute(queries["bookings_per_hour"]).df()
if not df_hour.empty:
    df_hour['hour'] = df_hour['hour'].astype(str)
    # find index of max
    max_idx = df_hour['unique_bookings'].idxmax()
    max_hour = df_hour.loc[max_idx, 'hour']
    max_count = int(df_hour.loc[max_idx, 'unique_bookings'])
    print(f"Hour with most bookings: {max_hour} ({max_count})")
    plot_bar(df_hour, x='hour', y='unique_bookings',
             title=f"Unique Bookings per Hour (top: {max_hour})",
             xlabel="Hour of day", ylabel="Unique bookings", rotate=0,
             highlight_idx=int(max_idx), out_name="bookings_per_hour.png")

# 1.b) bookings per weekday
df_weekday = con.execute(queries["bookings_per_weekday"]).df()
if not df_weekday.empty:
    # ensure order Sunday..Saturday by weekday_num
    df_weekday = df_weekday.sort_values('weekday_num')
    plot_bar(df_weekday, x='weekday_name', y='unique_bookings',
             title="Unique Bookings by Weekday",
             xlabel="Weekday", ylabel="Unique bookings", rotate=45,
             out_name="bookings_per_weekday.png")

# 2) bookings per month and top month
df_month = con.execute(queries["bookings_per_month"]).df()
if not df_month.empty:
    # find top month
    top_idx = df_month['bookings'].idxmax()
    top_month = df_month.loc[top_idx, 'month']
    top_count = int(df_month.loc[top_idx, 'bookings'])
    print(f"Month with most bookings: {top_month} ({top_count})")
    plot_bar(df_month, x='month', y='bookings',
             title=f"Bookings per Month (top: {top_month})",
             xlabel="Month", ylabel="Bookings", rotate=45,
             highlight_idx=int(top_idx), out_name="bookings_per_month.png")

# 3) booking status breakdown
df_status = con.execute(queries["booking_status_breakdown"]).df()
if not df_status.empty:
    plot_bar(df_status, x='Booking Status', y='bookings',
             title="Bookings by Booking Status",
             xlabel="Booking Status", ylabel="Bookings", rotate=45,
             out_name="booking_status_breakdown.png")

# 4) top customer and their payment methods
df_top_customer = con.execute(queries["top_customer_and_payment_methods"]).df()
if not df_top_customer.empty:
    customer_id = df_top_customer.loc[0, 'customer_id']
    total_for_customer = int(df_top_customer['bookings_for_method'].sum())
    title = f"Top customer: {customer_id} — payment methods (total {total_for_customer})"
    # plot horizontal bars for readability
    plt.figure(figsize=(8, 5))
    df_top_customer.plot(kind='barh', x='payment_method', y='bookings_for_method', legend=False, color='#2ca02c')
    plt.title(title)
    plt.xlabel("Bookings for method")
    plt.ylabel("Payment Method")
    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, "top_customer_payment_methods.png")
    plt.savefig(out)
    plt.close()
    print("Generated:", out)
