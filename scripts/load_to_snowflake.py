import pandas as pd
import snowflake.connector

# Load your CSV file
df = pd.read_csv('../data/retail_sales_dataset.csv')

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='TEBOHO',
    password='',
    account='',  
    warehouse='COMPUTE_WH',
    database='RETAIL_DB',
    schema='RAW'
)
cursor = conn.cursor()

# Insert data row-by-row
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO transactions_raw (
            Transaction_ID, Date, Customer_ID, Gender, Age,
            Product_Category, Quantity, Price_per_Unit, Total_Amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row['Transaction ID'], row['Date'], row['Customer ID'],
            row['Gender'], int(row['Age']), row['Product Category'],
            int(row['Quantity']), float(row['Price per Unit']), float(row['Total Amount'])
        )
    )

cursor.close()
conn.close()
print("✅ Data loaded into Snowflake successfully.")
