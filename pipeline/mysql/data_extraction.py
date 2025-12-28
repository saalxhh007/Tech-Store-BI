import mysql.connector as cn
from pprint import pprint

try:
    db = cn.connect(
    user="student_user_4ing",
    password="bi_guelma_2025",
    host="boughida.com",
    database="techstore_erp"
)
except cn.Error as e:
    print(e)

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

cursor = db.cursor(dictionary=True)

for name, table in tables.items():
    try:
        cursor.execute(f"SELECT * FROM {table} ORDER BY RAND() LIMIT 1")
        row = cursor.fetchone()
        print(f"Random {name} row:")
        pprint(row)
        print("-" * 50)
    except cn.Error as e:
        print(f"Error fetching from {table}:", e)