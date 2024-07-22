import duckdb
import pandas as pd

# In-memory database
con = duckdb.connect()

# Persistent database
con = duckdb.connect('mydatabase.db')
#con.execute("CREATE TABLE sales (order_id INTEGER, product STRING, quantity INTEGER, price_each DOUBLE, order_date TIMESTAMP, purchase_address STRING)")

# Inserting data
#con.execute("INSERT INTO sales VALUES (1, 'Product A', 2, 19.99, '2023-01-01 10:00:00', '123 Main St')")
#con.execute("INSERT INTO sales VALUES (1, 'Product B', 3, 29.99, '2024-01-01 10:00:00', '123 Main St')")

# Querying data into a Pandas dataframe
#df = con.execute("SELECT * FROM sales").fetchdf()
#print(df)

# Performing aggregations
#revenue_per_product = con.execute("SELECT product, SUM(price_each * quantity) AS total_revenue FROM sales GROUP BY product").fetchdf()
#print(revenue_per_product)

con.execute("COPY sales TO 'sales.parquet' (FORMAT PARQUET)")

# Reading from a Parquet file
parquet_df = con.execute("SELECT * FROM 'sales.parquet'").fetchdf()
parquet_file_path = 'C:/Users/kevin/OneDrive/Desktop/youtube_scripts/sales.parquet'

#Read Parquet
select_parquet_query = f"SELECT * FROM '{parquet_file_path}'"
parquet_data = con.execute(select_parquet_query).fetchdf()
print("Parquet File Data:")
print(parquet_data)