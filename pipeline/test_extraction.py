from pprint import pprint
import glob

from data_extraction import DataExtraction

db_config = {
    "user": "student_user_4ing",
    "password": "bi_guelma_2025",
    "host": "boughida.com",
    "database": "techstore_erp"
}

tables = {
    "sales": "table_sales",
    "products": "table_products",
    "reviews": "table_reviews",
    "categories": "table_categories",
    "subcategories": "table_subcategories",
    "stores": "table_stores",
    "customers": "table_customers",
    "stores": "table_cities",
}

excel_paths = {
    "marketing": "./../data/excel/marketing_expenses.xlsx",
    "targets": "./../data/excel/monthly_targets.xlsx",
    "shipping": "./../data/excel/shipping_rates.xlsx"
}

image_paths = glob.glob("./../data/legacy invoices/*.jpg")

test_url = "https://boughida.com/competitor/"

roi_positions = {
    "info_roi": (100, 250, 30, 550),
    "products_roi": (300, 400, 30, 550)
}

extractor = DataExtraction(
    db_config,
    OCR_config="--oem 3 --psm 6",
    ROI_positions=roi_positions
)

print("\n========== DATA EXTRACTION TEST ==========\n")

print("---- MySQL Extraction ----")
mysql_data = extractor.mysql_extraction(tables)
print("\n--- MYSQL (SAMPLES) ---")
for table, rows in mysql_data.items():
    print(f"\nTable: {table}")
    pprint(rows[:3])

print("\n---- Excel Extraction ----")
excel_data = extractor.excel_extraction(excel_paths)
for name, df in excel_data.items():
    print(f"\n{name.upper()}")
    print(df.head(3))


print("\n---- OCR Image Pipeline ----")
ocr_results = extractor.image_pipeline(image_paths)

for result in ocr_results:
    print("\nFile:", result["file"])
    print("INFO:")
    print(result["info"])
    print("PRODUCT:")
    print(result["product"])

print("\n---- Web Scraping ----")
products = extractor.webscraping(test_url)
pprint(products[:5])