import snowflake.connector

# Replace with your real values
conn = snowflake.connector.connect(
    user='TEBOHO',
    password='',
    account='',  
    warehouse='COMPUTE_WH',
    database='RETAIL_DB',
    schema='RAW'
)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER(), CURRENT_DATE;")
    result = cursor.fetchone()
    print(f"✅ Connected as {result[0]}, today's date is {result[1]}")
except Exception as e:
    print("❌ Connection failed:")
    print(e)
finally:
    cursor.close()
    conn.close()
