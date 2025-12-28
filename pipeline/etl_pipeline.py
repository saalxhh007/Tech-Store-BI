import pandas as pd
import mysql.connector
import requests
from bs4 import BeautifulSoup
import pytesseract
from PIL import Image
import os
import re
from sqlalchemy import create_engine

# =========================================================
# Stage 1: ERP Data Extraction (MySQL)
# =========================================================

db_config = {
    "host": "boughida.com",
    "user": "student_user_4ing",
    "password": "bi_guelma_2025",
    "database": "techstore_erp"
}

conn = mysql.connector.connect(**db_config)

if conn.is_connected():
    print("✅ Connection to MySQL successful")
else:
    raise ConnectionError("❌ MySQL connection failed")

tables = {
    "sales": "table_sales",
    "products": "table_products",
    "reviews": "table_reviews",
    "categories": "table_categories",
    "subcategories": "table_subcategories",
    "stores": "table_stores",
    "customers": "table_customers",
    "cities": "table_cities"
}

dataframes = {}

for name, table in tables.items():
    query = f"SELECT * FROM {table}"
    dataframes[name] = pd.read_sql(query, conn)
    print(f"{name} extracted:", dataframes[name].shape)

df_sales = dataframes["sales"]
df_products = dataframes["products"]
df_reviews = dataframes["reviews"]
df_categories = dataframes["categories"]
df_subcategories = dataframes["subcategories"]
df_stores = dataframes["stores"]
df_customers = dataframes["customers"]
df_cities = dataframes["cities"]

conn.close()
print("🔒 MySQL connection closed")

# =========================================================
# Stage 2: Departmental Data Extraction (Excel)
# =========================================================

df_marketing = pd.read_excel("flat_files/marketing_expenses.xlsx")
df_targets = pd.read_excel("flat_files/monthly_targets.xlsx")
df_shipping = pd.read_excel("flat_files/shipping_rates.xlsx")

print("Marketing:", df_marketing.shape)
print("Targets:", df_targets.shape)
print("Shipping:", df_shipping.shape)

# =========================================================
# Stage 3: Competitor Pricing (Web Scraping)
# =========================================================

url = "https://boughida.com/competitor/"
response = requests.get(url, timeout=10)
print("Status Code:", response.status_code)

soup = BeautifulSoup(response.text, "html.parser")
products = soup.find_all("div", class_="product")
print("Products found:", len(products))

competitor_data = []

for product in products:
    try:
        name = product.find("h3").get_text(strip=True)
        price_text = product.find("span", class_="price").get_text(strip=True)

        price = float(
            price_text.replace("DZD", "").replace(",", "").strip()
        )

        competitor_data.append({
            "Competitor_Product_Name": name,
            "Competitor_Price": price
        })
    except Exception:
        continue

df_competitor_prices = pd.DataFrame(competitor_data)

# =========================================================
# Stage 4: Legacy Sales Data (OCR – 2022)
# =========================================================

invoice_folder = "legacy_invoices"
invoice_texts = []

if os.path.exists(invoice_folder):
    for file in os.listdir(invoice_folder):
        if file.lower().endswith(".jpg"):
            image = Image.open(os.path.join(invoice_folder, file))
            text = pytesseract.image_to_string(image)
            invoice_texts.append({"file": file, "raw_text": text})

print("Invoices processed:", len(invoice_texts))

legacy_sales = []

for invoice in invoice_texts:
    text = invoice["raw_text"]

    date_match = re.search(r"(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})", text)
    quantity_match = re.search(r"Quantity\s*[:\-]?\s*(\d+)", text, re.IGNORECASE)
    total_match = re.search(r"Total\s*[:\-]?\s*([\d,]+)", text, re.IGNORECASE)
    product_match = re.search(r"Product\s*[:\-]?\s*(.+)", text, re.IGNORECASE)

    legacy_sales.append({
        "date": date_match.group(1) if date_match else None,
        "product_name": product_match.group(1).strip() if product_match else None,
        "quantity": int(quantity_match.group(1)) if quantity_match else None,
        "total_revenue": int(total_match.group(1).replace(",", "")) if total_match else None,
        "customer_id": None
    })

df_legacy_sales = pd.DataFrame(legacy_sales)

# =========================================================
# Stage 5: Data Cleaning & Quality Control
# =========================================================

def clean_columns_safe(df):
    df.columns = df.columns.astype(str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

for df in [
    df_sales, df_products, df_reviews, df_categories, df_subcategories,
    df_stores, df_customers, df_cities,
    df_marketing, df_targets, df_shipping,
    df_competitor_prices, df_legacy_sales
]:
    clean_columns_safe(df)

df_sales["date"] = pd.to_datetime(df_sales["date"], errors="coerce")
df_legacy_sales["date"] = pd.to_datetime(df_legacy_sales["date"], errors="coerce")

if "month" in df_targets.columns:
    df_targets["month"] = pd.to_datetime(df_targets["month"], errors="coerce")

for col in ["quantity", "total_revenue"]:
    if col in df_legacy_sales.columns:
        df_legacy_sales[col] = pd.to_numeric(df_legacy_sales[col], errors="coerce")

df_sales = df_sales[df_sales["quantity"] > 0]
df_legacy_sales = df_legacy_sales[df_legacy_sales["total_revenue"] > 0]

df_sales.drop_duplicates(inplace=True)
df_products.drop_duplicates(inplace=True)
df_customers.drop_duplicates(inplace=True)
df_legacy_sales.drop_duplicates(inplace=True)
df_competitor_prices.drop_duplicates(inplace=True)

# =========================================================
# Stage 6: Data Transformation & Integration
# =========================================================

df_sales["source"] = "ERP"
df_legacy_sales["source"] = "LEGACY"

sales_all = pd.concat([
    df_sales[["date", "product_name", "quantity", "total_revenue", "customer_id", "source"]],
    df_legacy_sales[["date", "product_name", "quantity", "total_revenue", "customer_id", "source"]]
], ignore_index=True)

# Dimensions
dim_date = pd.DataFrame({"date": sales_all["date"].dropna().unique()})
dim_date["year"] = dim_date["date"].dt.year
dim_date["month"] = dim_date["date"].dt.month
dim_date["day"] = dim_date["date"].dt.day
dim_date["quarter"] = dim_date["date"].dt.quarter

dim_product = df_products[["product_id", "product_name", "subcategory_id", "price"]].copy()
dim_customer = df_customers[["customer_id", "customer_name", "city_id"]].copy()
dim_store = df_stores[["store_id", "store_name", "city_id"]].copy()
dim_city = df_cities[["city_id", "city_name", "country"]].copy()

# Fact table
fact_sales = sales_all.merge(dim_date, on="date", how="left")
fact_sales = fact_sales.merge(dim_product[["product_id", "product_name"]], on="product_name", how="left")
fact_sales = fact_sales.merge(dim_customer, on="customer_id", how="left")

fact_sales = fact_sales[[
    "date", "product_id", "customer_id",
    "quantity", "total_revenue", "source"
]]

print("===== STAR SCHEMA SUMMARY =====")
print("Fact Sales:", fact_sales.shape)
print("Dim Date:", dim_date.shape)
print("Dim Product:", dim_product.shape)
print("Dim Customer:", dim_customer.shape)
print("Dim Store:", dim_store.shape)
print("Dim City:", dim_city.shape)
