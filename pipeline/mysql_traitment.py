import mysql.connector
try:
    db = mysql.connector.connect(
    host="boughida.com",
    user="student_user_4ing",
    password="bi_guelma_2025",
    database="techstore_erp"
    )
except mysql.connector.Error as err:
    print(err)

cursor = db.cursor()
cursor.execute("SELECT * FROM table_sales")
cursor.execute("SELECT * FROM table_products")
# cursor.execute("SELECT * FROM table_reviews")
# cursor.execute("SELECT * FROM table_subcategories")
# cursor.execute("SELECT * FROM table_categories")
# cursor.execute("SELECT * FROM table_stores")
# cursor.execute("SELECT * FROM table_customers")
# cursor.execute("SELECT * FROM table_cities")

res = cursor.fetchall()
for row in res:
    print(row)
cursor.close()
db.close()