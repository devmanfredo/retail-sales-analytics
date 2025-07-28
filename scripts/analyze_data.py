import snowflake.connector
import pandas as pd

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='TEBOHO',
    password='Hotboi$ummer2002',
    account='cj27283.af-south-1.aws',
    warehouse='COMPUTE_WH',
    database='RETAIL_DB',
    schema='RAW'
)

# Query your Snowflake table
query = "SELECT * FROM transactions_raw;"
cursor = conn.cursor()
cursor.execute(query)

# Load data into pandas DataFrame
df = pd.read_sql(query, conn)

# Normalize column names to lowercase
df.columns = df.columns.str.lower()

#Show first few transactions
print("📊 First few transactions:")
print(df.head())

#Total sales per product category
category_sales = df.groupby('product_category')['total_amount'].sum().reset_index()
print("\n💰 Total Sales by Product Category:")
print(category_sales)

# Average basket size per customer
avg_basket = df.groupby('customer_id')['total_amount'].sum().reset_index()
avg_basket.columns = ['customer_id', 'total_spent']
print("\n🧺 Average Basket Size (Top 5 Customers):")
print(avg_basket.sort_values(by='total_spent', ascending=False).head())

#Gender distribution of customers
gender_dist = df.groupby('gender').size().reset_index(name='count')
print("\n🚻 Gender Distribution:")
print(gender_dist)

#Save to CSV for Power BI
category_sales.to_csv('../outputs/category_sales.csv', index=False)
avg_basket.to_csv('../outputs/avg_basket.csv', index=False)
gender_dist.to_csv('../outputs/gender_distribution.csv', index=False)

# Close connections
cursor.close()
conn.close()
