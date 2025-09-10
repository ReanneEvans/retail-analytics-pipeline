import pandas as pd
from sqlalchemy import create_engine

user = 'your_user'  
password = 'your_password'  
host = 'localhost'
port = '3306'
database = 'sales_dashboard'

# Create the SQLAlchemy engine for MySQL
connection_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_str)
)
engine = create_engine(url)

# --- SALES TABLE CLEANING ---

# Load the Sales data
sales_df = pd.read_csv("Sales.csv")

# size of dataset
print(sales_df.shape)

#column names 
print(sales_df.columns)

#data types 
print(sales_df.dtypes)

#basic stats
print(sales_df.describe())

#dates
sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'])
sales_df['Delivery Date'] = pd.to_datetime(sales_df['Delivery Date'])
print(sales_df[['Order Date', 'Delivery Date']].head())
print(sales_df.dtypes)  # To confirm conversion

# add column 
sales_df['Delivery Time'] = sales_df['Delivery Date'] - sales_df['Order Date']
print(sales_df[['Order Date', 'Delivery Date', 'Delivery Time']].head())

#mean 
avg_delivery_time = sales_df['Delivery Time'].mean()
print(f'Average delivery time: {avg_delivery_time}')

#check for nulls 
print(sales_df.isnull().sum())

#add delivered flag
sales_df['delivered'] = sales_df['Delivery Date'].notnull()
print(sales_df['delivered'].value_counts())

#check columns for unique values 
print(sales_df['Currency Code'].value_counts())

#check for duplicates 
duplicate_rows= sales_df.duplicated(subset=['Order Number','Line Item'])
print(f"Number of duplicated items:{duplicate_rows.sum()}")

#check unique or invalid values 
print(sales_df['Quantity'].value_counts().sort_index())

#replace column names ready for sql 
sales_df.rename(columns={
    'Order Number': 'order_number',
    'Line Item': 'line_item',
    'Order Date': 'order_date',
    'Delivery Date': 'delivery_date',
    'CustomerKey': 'customer_key',
    'StoreKey': 'store_key',
    'ProductKey': 'product_key',
    'Quantity': 'quantity',
    'Currency Code': 'currency_code',
    'Delivery Time': 'delivery_time',
    'delivered': 'delivered'  # already lowercase, but included for consistency
}, inplace=True)
print(sales_df.columns)

# --- PRODUCTS TABLE CLEANING ---

products_df = pd.read_csv('Products.csv')
print(products_df.head())       # Peek at the first few rows
print(products_df.shape)        # Check number of rows and columns
print(products_df.columns)      # See column names
print(products_df.dtypes)       # Check data types


# Remove $ signs and convert to float
products_df['Unit Cost USD'] = products_df['Unit Cost USD'].replace('[\$,]', '', regex=True).astype(float)
products_df['Unit Price USD'] = products_df['Unit Price USD'].replace('[\$,]', '', regex=True).astype(float)

# Confirm change
print(products_df[['Unit Cost USD', 'Unit Price USD']].dtypes)
print(products_df[['Unit Cost USD', 'Unit Price USD']].head())

# check for missing values 
print(products_df.isnull().sum())

# check duplicates 
duplicates = products_df.duplicated()
print(duplicates.sum())  # How many duplicate rows are there?

# Find rows where cost or price is zero or less
suspicious = products_df[(products_df['Unit Cost USD'] <= 0) | (products_df['Unit Price USD'] <= 0)]

print(suspicious)
print(f"Number of suspicious rows: {len(suspicious)}")

# Clean column names: lowercase and replace spaces with underscores
products_df.columns = products_df.columns.str.strip().str.lower().str.replace(' ', '_')
products_df.rename(columns={'productkey':'product_key','subcategorykey':'subcategory_key','categorykey':'category_key'}, inplace=True)

# Confirm the new column names
print(products_df.columns)

# --- CUSTOMERS TABLE CLEANING ---

customers_df = pd.read_csv('Customers.csv', encoding='latin1')

# Quick scan
print(customers_df.head())        # Preview first few rows
print(customers_df.shape)         # Rows and columns
print(customers_df.columns)       # Column names
print(customers_df.dtypes)        # Data types

customers_df.columns = customers_df.columns.str.strip().str.lower().str.replace(' ', '_')
customers_df.rename(columns={'customerkey':'customer_key'}, inplace=True)

print(customers_df.columns)

# fix birthday 
customers_df['birthday'] = pd.to_datetime(customers_df['birthday'], errors='coerce')
print(customers_df['birthday'].dtypes)
print(customers_df['birthday'].head())

#missing values
print(customers_df.isnull().sum())

print(customers_df[customers_df['state_code'].isnull()])

# check duplicates 
duplicates = customers_df[customers_df.duplicated()]
print(duplicates)
print(f"Number of full duplicates: {len(duplicates)}")

#checking duplicate key values as should be unique 
duplicate_keys = customers_df[customers_df.duplicated(subset=['customer_key'], keep=False)]
print(duplicate_keys)
print(f"Number of duplicate customer keys: {len(duplicate_keys)}")

# standardise gender 
print(customers_df['gender'].unique())

# suspious zip codes 
print(customers_df['zip_code'].unique()[:20])  # Show first 20 unique zip codes

non_numeric_zips = customers_df[~customers_df['zip_code'].str.isdigit()]
print(non_numeric_zips[['zip_code', 'country']])

# Count missing (NaN) zip codes
missing_zip_count = customers_df['zip_code'].isna().sum()

# Count empty strings or whitespace-only zip codes
empty_zip_count = customers_df['zip_code'].str.strip().eq('').sum()

print(f"Missing zip codes: {missing_zip_count}")
print(f"Empty zip codes: {empty_zip_count}")

# --- STORES TABLE CLEANING ---

stores_df = pd.read_csv('Stores.csv')

print(stores_df.head())        # Preview first few rows
print(stores_df.shape)         # Rows and columns
print(stores_df.columns)       # Column names
print(stores_df.dtypes)        # Data types

# fix column names 
stores_df.columns = stores_df.columns.str.strip().str.lower().str.replace(' ', '_')
stores_df.rename(columns={'storekey':'store_key'}, inplace=True)
print(stores_df.columns)

# fix date 
stores_df['open_date'] = pd.to_datetime(stores_df['open_date'], errors='coerce')
print(stores_df['open_date'].dtypes)
print(stores_df['open_date'].head())

#missing values 
print(stores_df.isnull().sum())

print(stores_df[stores_df['square_meters'].isnull()])

#check duplicates 
print(stores_df.duplicated().sum())

#checking key values are unique
print(stores_df['store_key'].duplicated().sum())

# check suspisous 
physical_stores = stores_df[stores_df['state'] != 'Online']

suspicious_space = physical_stores[physical_stores['square_meters'] <= 0]
print(suspicious_space)

print(stores_df[stores_df['open_date'].isnull()])

#check if any dates in future
from datetime import datetime

today = datetime.today()
future_stores = stores_df[stores_df['open_date'] >= today]
print(future_stores)

print("Countries:", stores_df['country'].unique())
print("States:", stores_df['state'].unique())


# --- EXCHANGE RATE TABLE CLEANING ---

exchange_df = pd.read_csv('Exchange_Rates.csv')

# Quick scan
print(exchange_df.head())
print(exchange_df.shape)
print(exchange_df.columns)
print(exchange_df.dtypes)

exchange_df.columns = exchange_df.columns.str.strip().str.lower().str.replace(' ', '_')
print(exchange_df.columns)

# change date format 

exchange_df['date'] = pd.to_datetime(exchange_df['date'], errors='coerce')
print(exchange_df['date'].dtypes)
print(exchange_df['date'].head())

#missing values 
print(exchange_df.isnull().sum())

#duplicates 
duplicate_rows = exchange_df.duplicated()
print(f"Full duplicate rows: {duplicate_rows.sum()}")

# Check for duplicate currency-date pairs
duplicate_pairs = exchange_df.duplicated(subset=['date', 'currency'])
print(f"Duplicate currency-date pairs: {duplicate_pairs.sum()}")

# suspisous rates 
suspicious_rates = exchange_df[exchange_df['exchange'] <= 0]
print(suspicious_rates)
print(f"Number of suspicious exchange rates: {len(suspicious_rates)}")

# --- EXPORT TO MYSQL ---
sales_df.to_sql('sales', con=engine, if_exists='replace', index=False)
products_df.to_sql('products', con=engine, if_exists='replace', index=False)
customers_df.to_sql('customers', con=engine, if_exists='replace', index=False)
stores_df.to_sql('stores', con=engine, if_exists='replace', index=False)
exchange_df.to_sql('exchange_rates', con=engine, if_exists='replace', index=False)

print("✅ All DataFrames exported to MySQL successfully.")

