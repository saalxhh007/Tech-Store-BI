import pandas as pd
import mysql.connector as cn
import cv2
import pytesseract
import requests
from bs4 import BeautifulSoup
        
class DataExtraction():
    def __init__(self, db_config, OCR_config, ROI_positions):
        self.db_config = db_config
        self.OCR_config = OCR_config
        self.ROI_positions = ROI_positions

        self.db = None
        if db_config:
            self._connect_db()
    # Helper Funcs
    # image Cleaning before traitment
    def img_preprocess(self, roi):
        gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)

        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        gray = clahe.apply(gray)
        gray = cv2.GaussianBlur(gray, (3,3), 0)

        thresh = cv2.adaptiveThreshold(
            gray, 255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            15, 3
        )
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2,2))
        thresh = cv2.dilate(thresh, kernel, iterations=1)
        return thresh

    def correct_ocr_text(self,text, field):
        text = text.strip()
        if field == "product_name":
            corrections = {
                "Alr": "Air",
                "Uiva": "Ultra",
                "Oeil": "Dell",
                "$23": "S23",
                "xPS": "XPS"
            }
            for wrong, right in corrections.items():
                text = text.replace(wrong, right)
        elif field in ["order_id", "quantity", "unit_price", "total_price", "client_id"]:
            text = ''.join([c for c in text if c.isdigit() or c == "C"])
        return text

    # connect to the database
    def _connect_db(self):
        try:
            self.db = cn.connect(**self.db_config)
        except cn.Error as e:
            print("DB connection error:", e)

    def mysql_extraction(self, tables):
        res = {}
        if not self.db:
            return {}
        
        cursor = self.db.cursor(dictionary=True)

        for name, table in tables.items():
            try:
                cursor.execute(f"SELECT * FROM {table}")
                res[name] = cursor.fetchall()
                # print(name)
            except cn.Error as e:
                print(f"Error fetching from {table}:", e)

        return res

    def excel_extraction(self, paths):
        return {
            "marketing_df": pd.read_excel(paths["marketing"]),
            "targets_df": pd.read_excel(paths["targets"]),
            "shipping_df": pd.read_excel(paths["shipping"]),
        }
    
    # Extract data from image {file , personal_info: ID, Name... , product_info: Name, Quantity, Unit & Total Price}
    def image_pipeline(self, img_paths):
        extracted = []

        for path in img_paths:
            image = cv2.imread(path)
            if image is None:
                print("Error: Could not load image")
                continue

            record = {"file": path}

            for field_name, (y1, y2, x1, x2) in self.ROI_positions.items():
                roi = image[y1:y2, x1:x2]

                text = pytesseract.image_to_string(
                    self.img_preprocess(roi),
                    config=self.OCR_config
                )

                corrected_text = self.correct_ocr_text(text, field_name)
                record[field_name] = corrected_text

                # cv2.rectangle(image, (x1, y1), (x2, y2), (255, 0, 0), 2)

            extracted.append(record)

            # cv2.imshow("ROIs", image)
            # cv2.waitKey(0)
            # cv2.destroyAllWindows()

        return extracted
    # Extract data from web {Title, ID, Old Price, Price}
    def webscraping(self, url):
        products = []

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print("Request failed:", e)
            exit()

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            # print(product_cards)
            for product_card in soup.select("div.product-card"):
                # class="price-tag product-price"
                # class="price-tag product-price" new price
                products.append({  
                    "Title": product_card.find("h5").get_text(strip=True) if product_card.find("h5") else "",
                    "ID": product_card.find("p").get_text(strip=True) if product_card.find("p") else "",
                    "Old Price": product_card.find("span", class_="old-price").get_text(strip=True) if product_card.find("span", class_="old-price") else "",
                    "Price": product_card.find("span", class_="product-price").get_text(strip=True) if product_card.find("span", class_="product-price") else "",
                })
        return products