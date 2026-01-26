import decimal
import re
import datetime

import pandas as pd
from sklearn.impute import SimpleImputer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

USD_TO_DZD = 129.51

class Data_cleaning():
    def __init__(self, excel_data, image_data, web_data, mysql_data):
        self.excel_data = excel_data
        self.image_data = image_data
        self.web_data = web_data
        self.mysql_data = mysql_data

        self.analyzer = SentimentIntensityAnalyzer()
    # Helper's
    # Remove The Duplicate Row's
    def remove_duplicates(self, df):
        return df.drop_duplicates()
    # convert Numb To Absolute values
    def abs_numbers(self, df, col):
        return pd.to_numeric(df[col], errors="coerce").abs()
    # Replace With Cost/Most Frequent
    def imputer_numbers(self, df, col):
        cost_imputer = SimpleImputer(strategy="mean")
        return cost_imputer.fit_transform(df[[col]])
    def imputer_text(self, df, col):
        cost_imputer = SimpleImputer(strategy="most_frequent")
        return cost_imputer.fit_transform(df[[col]])
    def clean_datetime_column(self, df, column_name, add_month_column=False, default_date="2023-01-01"):
        df_clean = df.copy()

        df_clean[column_name] = (
            df_clean[column_name]
            .astype(str)
            .str.strip()
            .replace(["", "nan", "NaN"], pd.NA)
        )

        df_clean[column_name] = pd.to_datetime(
            df_clean[column_name],
            errors="coerce",
            dayfirst=True
        )


        df_clean[column_name] = df_clean[column_name].fillna(pd.to_datetime(default_date))

        if add_month_column:
            df_clean[column_name + "_month"] = df_clean[column_name].dt.to_period("M")

        return df_clean
    def clean_numeric_nan_and_negative(self, df):
        df_clean = df.copy()

        numeric_cols = df_clean.select_dtypes(include=['number']).columns

        df_clean[numeric_cols] = (
            df_clean[numeric_cols]
            .fillna(0)
            .clip(lower=0)
        )

        return df_clean
    def clean_categorical_column(self, df, column_name):
        df_clean = df.copy()

        df_clean[column_name] = (
            df_clean[column_name]
            .astype(str)
            .str.strip()      
            .str.lower()       
            .str.capitalize()
        )

        return df_clean
    def clean_store_id(self, df, column_name):
        df_clean = df.copy()

        df_clean[column_name] = (
            df_clean[column_name]
            .astype(str)
            .str.extract(r"(\d+)")
            .astype(int)
        )

        return df_clean   
    def clean_ID(self, df, col):
        df[col] = (
            df[col]
            .astype(str)
            .apply(lambda x: re.sub(r"\D", "", x))
        )
        return pd.to_numeric(df[col], errors="coerce")
    def clean_revenue_column(self, df, column_name):
        df_clean = df.copy()

        df_clean[column_name] = (
            df_clean[column_name]
            .astype(str)
            .str.replace(",", "", regex=False)
            .astype(float)
        )

        return df_clean
    def clean_text_column(self, df, column_name):
       df_clean = df.copy()

       df_clean[column_name] = (
           df_clean[column_name]
           .astype(str)
           .str.strip()
           .str.title()
       )

       return df_clean
    def clean_web_scraping_data(self, df):
        df_clean = df.copy()

        df_clean['Price'] = (
            df_clean['Price']
            .astype(str)              
            .str.replace(' DZD', '', regex=False)  
            .str.strip()                
            .replace('', '0')           
            .astype(float)            
        )

        df_clean['Old Price'] = (
            df_clean['Old Price']
            .astype(str)
            .str.replace(' DZD', '', regex=False)
            .str.strip()
            .replace('', '0')
            .astype(float)
        )

        df_clean['Product_ID'] = df_clean['ID'].str.extract(r'(P-\d+)')

        return df_clean
    def convert_usd_to_dzd(self, df, column_usd, new_column=None):

        if new_column is None:
            new_column = "Marketing_Cost_DZD"

        df[new_column] = df[column_usd] * USD_TO_DZD
        return df
    def convert_decimal_to_float(self ,df):

        for col in df.columns:
            if df[col].dtype == object:  
                if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
                    df[col] = df[col].apply(float)
        return df
    # Helper For Sentiment Analyzer
    def clean_text(self, text: str) -> str:
        text = text.lower() 
        text = re.sub(r"http\S+", "", text)
        text = re.sub(r"[^a-z\s]", " ", text) 
        text = re.sub(r"\s+", " ", text)
        return text.strip()
     
    def _get_sentiment_score(self, text):
        vs = self.analyzer.polarity_scores(text)
        return vs["compound"]
    # Cleaning & Transforming The Excel Files
    def clean_transform_excel(self):
        cleaned_dfs = {}
        for name, df in self.excel_data.items():
            df = df.copy()
            
            df = self.remove_duplicates(df)
            if name.upper() == "MARKETING_DF":
                df = self.clean_datetime_column(df, "Date")
                df = self.clean_categorical_column(df, "Category")
                df = self.clean_categorical_column(df, "Campaign_Type")
                
                df["Marketing_Cost_USD"] = self.abs_numbers(df, "Marketing_Cost_USD")
                
                df[["Marketing_Cost_USD"]] = self.imputer_numbers(df, "Marketing_Cost_USD")
                df[["Category"]] = self.imputer_text(df, "Category")
                df[["Campaign_Type"]] = self.imputer_text(df, "Campaign_Type")

                df = self.convert_usd_to_dzd(df, "Marketing_Cost_USD", "Marketing_Cost_DZD")
                df["Marketing_Cost_DZD"] = df["Marketing_Cost_DZD"].round(2)

            elif name.upper() == "TARGETS_DF":
                df = self.clean_datetime_column(df, "Month")
                df = self.clean_categorical_column(df, "Manager_Name")
                df = self.clean_store_id(df, "Store_ID")

                df = self.clean_revenue_column(df, "Target_Revenue")
                df["Target_Revenue"] = df["Target_Revenue"].abs()
                    
                df = self.clean_text_column(df, "Manager_Name")

                df[["Target_Revenue"]] = self.imputer_numbers(df, "Target_Revenue")
                df[["Manager_Name"]] = self.imputer_text(df, "Manager_Name")
                    
            cleaned_dfs[name.upper()] = df
        return cleaned_dfs
    
    # Sentiment Analyzer
    def sentiment_analysis(self):
        if "reviews" not in self.mysql_data or not self.mysql_data["reviews"]:
            print("No reviews data")
            return {}

        product_scores = {}

        for row in self.mysql_data["reviews"]:
            product_id = row.get("Product_ID")
            review_text = row.get("Review_Text")

            if not product_id or not review_text:
                continue

            clean_text = self.clean_text(review_text)
            if clean_text == "":
                continue

            score = self._get_sentiment_score(clean_text)

            if product_id not in product_scores:
                product_scores[product_id] = []

            product_scores[product_id].append(score)

        # Average sentiment per product
        final_scores = {}
        for product_id, scores in product_scores.items():
            final_scores[product_id] = sum(scores) / len(scores)

        return final_scores

    # Net Profit CAlculation
    def calculate_net_profit(self):
        sales = self.mysql_data["sales"]
        products = self.mysql_data["products"]
        marketing_df = self.excel_data["marketing_df"]
        shipping_df = self.excel_data["shipping_df"] if "shipping_df" in self.excel_data else self.excel_data.get("SHIPPING_DF", None)
        
        product_cost_map = {
            p["Product_ID"]: float(p["Unit_Cost"])
            for p in products
        }

        product_category_map = {p["Product_ID"]: p.get("Category", "Unknown") for p in products}
        shipping_map = {}
        if shipping_df is not None:
            shipping_map = shipping_df.groupby("region_name")["shipping_cost"].mean().to_dict()
            
        store_region_map = {}

        targets_df = self.excel_data.get("TARGETS_DF") or self.excel_data.get("targets_df")
        if targets_df is not None and "Store_ID" in targets_df.columns and "region_name" in targets_df.columns:
            store_region_map = targets_df.set_index("Store_ID")["region_name"].to_dict()
                    
        marketing_df["Month"] = pd.to_datetime(marketing_df["Date"]).dt.to_period("M")
        marketing_lookup = marketing_df.groupby(["Category", "Month"])["Marketing_Cost_USD"].mean().to_dict()
            
        net_profit_per_sale = []
        for s in sales:
            product_id = s["Product_ID"]
            store_id = s.get("Store_ID")
            quantity = s["Quantity"]
            total_revenue = float(s["Total_Revenue"])
            unit_cost = product_cost_map.get(product_id, 0.0)
            region = store_region_map.get(store_id)
            shipping_cost = shipping_map.get(region, 0.0)

            category = product_category_map.get(product_id, "Unknown")
            sale_month = pd.to_datetime(s["Date"]).to_period("M")
            marketing_cost = marketing_lookup.get((category, sale_month), 0.0)

            net_profit = total_revenue - (unit_cost * quantity) - shipping_cost - marketing_cost
            net_profit_per_sale.append({
                "Trans_ID": s["Trans_ID"],
                "Product_ID": product_id,
                "Store_ID": store_id,
                "Net_Profit": net_profit
            })
        
        return net_profit_per_sale

    def clean_transform_ocr(self):
        cleaned_records = []

        for rec in self.image_data:
            cleaned = {}
            cleaned["file"] = rec.get("file", "").strip()

            # Date
            raw_date = rec.get("date", "").strip()
            cleaned_date = raw_date
            try:
                raw_date = raw_date.replace(",", "").strip()
                dt = datetime.strptime(raw_date, "%Y-%m-%d")
                cleaned_date = dt.strftime("%Y-%m-%d")
            except:
                cleaned_date = raw_date
            cleaned["date"] = cleaned_date

            raw_order = rec.get("order_id", "").strip()

            # Order ID
            raw_order_digits = re.sub(r"\D", "", raw_order)

            if raw_order_digits != "":
                cleaned["order_id"] = f"ORD-{raw_order_digits}"
            else:
                cleaned["order_id"] = ""
                
            client_info = rec.get("client_info", "").strip()
            
            customer_id = ""
            full_name = ""
            if "Client" in client_info and "Nom" in client_info:
                try:
                    id_part = client_info.split("Client")[1]
                    id_part = re.findall(r"C\d+|\d+", id_part)
                    if id_part:
                        cid = id_part[0]
                        if not cid.upper().startswith("C"):
                            cid = f"C{cid}"
                        customer_id = cid

                    name_part = client_info.split("Nom:")[1].strip()
                    name_part = re.sub(r"\bProduit\b", "", name_part, flags=re.IGNORECASE)
                    name_part = name_part.split("\n")[0].strip()
                    full_name = name_part.title()

                except:
                    pass
            cleaned["customer_id"] = customer_id
            cleaned["full_name"] = full_name
            
            product_name = rec.get("product_name", "").strip()
            cleaned["product_name"] = product_name.title()
            
            qty_raw = rec.get("product_quantity", "").strip()
            qty_digits = re.sub(r"\D", "", qty_raw)
            cleaned["product_quantity"] = int(qty_digits) if qty_digits != "" else None
            
            unit_raw = rec.get("unit_price", "").strip()
            unit_digits = re.sub(r"\D", "", unit_raw)
            cleaned["unit_price"] = int(unit_digits) if unit_digits != "" else None
            
            total_raw = rec.get("total_price", "").strip()
            total_digits = re.sub(r"\D", "", total_raw)
            cleaned["total_price"] = int(total_digits) if total_digits != "" else None

            cleaned_records.append(cleaned)
        return cleaned_records    
    
    def clean_web_data(self):
        cleaned = []

        for row in self.web_data:
            new_row = row.copy()

            if "ID" in row and row["ID"]:
                match = re.search(r"P-\d+", row["ID"])
                new_row["ID"] = match.group(0) if match else row["ID"]

            for col in ["Price", "Old Price"]:
                val = row.get(col, "")
                if isinstance(val, str):
                    num = re.sub(r"[^\d.]", "", val)
                    new_row[col] = int(num) if num else None
                else:
                    new_row[col] = val

            title = row.get("Title", "")
            is_promo = 0
            if title.lower().startswith("promo:"):
                is_promo = 1
                title = title[6:].strip()
            elif title.lower().startswith("best deal:"):
                is_promo = 1
                title = title[10:].strip()

            new_row["Title"] = title
            new_row["is_promo"] = is_promo

            cleaned.append(new_row)

        return cleaned
