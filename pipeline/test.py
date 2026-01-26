from pipeline.DataCleaning import Data_cleaning
from pipeline.DataExtraction import DataExtraction
from pipeline.Data_Loading import DataLoading

import glob
from pprint import pprint

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
    "cities": "table_cities"
}

excel_paths = {
    "marketing": "./data/excel/marketing_expenses.xlsx",
    "targets": "./data/excel/monthly_targets.xlsx",
    "shipping": "./data/excel/shipping_rates.xlsx"
}

image_paths = glob.glob("./data/legacy invoices/*.jpg")

test_url = "https://boughida.com/competitor/"

roi_positions = {
    "date": (100, 160, 100, 230),
    "order_id": (100, 160, 430, 490),
    "client_info": (180, 280, 50,270),
    "product_name": (300, 350, 30, 200),
    "product_quantity": (300, 350, 290, 320),
    "unit_price": (300, 350, 345, 420),
    "total_price": (300, 350, 470, 550)
}

extractor = DataExtraction(
    db_config,
    OCR_config="--oem 3 --psm 6",
    ROI_positions=roi_positions
)

mysql_data = extractor.mysql_extraction(tables)
excel_data = extractor.excel_extraction(excel_paths)
image_data = extractor.image_pipeline(image_paths)
web_data = extractor.webscraping(test_url)
                    
transformer = Data_cleaning(excel_data, image_data, web_data, mysql_data)

ocr_transformed = transformer.clean_transform_ocr()
sentiment_analyzer = transformer.sentiment_analysis()
excel_transformed = transformer.clean_transform_excel()
# database_transformed = transformer.clean_database()
web_transformed = transformer.clean_web_data()
net_profit = transformer.calculate_net_profit()

loader = DataLoading(excel_transformed,
                    web_transformed, 
                    ocr_transformed, 
                    sentiment_analyzer, 
                    net_profit,
                    mysql_data
                    )

loader.load_fact_sales()
loader.load_dim_product()
loader.load_dim_customer()
loader.load_dim_store()
loader.load_dim_date()