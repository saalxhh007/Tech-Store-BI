import sqlite3
import pandas as pd
from decimal import Decimal

def convert_decimal_to_float(x):
    if isinstance(x, Decimal):
        return float(x)
    return x
class DataLoading():
    def __init__(self,
                excel_transformed,
                web_transformed, 
                ocr_transformed, 
                sentiment_analyzer, 
                net_profit,
                mysql_data
                ):
        self.excel_data = excel_transformed
        self.web_data = web_transformed
        self.ocr_data = ocr_transformed
        self.mysql_data = mysql_data
        self.sentiment_analyzer = sentiment_analyzer
        self.net_profit = net_profit

        self.conn = sqlite3.connect("techstore_dw.db")
        self.cursor = self.conn.cursor()

    def load_fact_sales(self):
        sales_df = pd.DataFrame(self.mysql_data["sales"])
        sales_df = sales_df[[
            "Trans_ID",
            "Product_ID",
            "Store_ID",
            "Quantity",
            "Total_Revenue"
        ]]

        net_profit_df = pd.DataFrame(self.net_profit)
        net_profit_df = net_profit_df[[
            "Trans_ID",
            "Net_Profit"
        ]]
        fact_sales_df = sales_df.merge(
            net_profit_df,
            on="Trans_ID",
            how="left"
        )
        fact_sales_df = fact_sales_df.apply(lambda col: col.map(convert_decimal_to_float))

        for col in ["Quantity", "Total_Revenue", "Net_Profit"]:
            fact_sales_df[col] = pd.to_numeric(fact_sales_df[col], errors="coerce")

        fact_sales_df["Trans_ID"] = pd.to_numeric(fact_sales_df["Trans_ID"], errors="coerce")
        fact_sales_df["Store_ID"] = pd.to_numeric(fact_sales_df["Store_ID"], errors="coerce")

        fact_sales_df["Product_ID"] = fact_sales_df["Product_ID"].astype(str)

        fact_sales_df = fact_sales_df.dropna(subset=["Trans_ID", "Product_ID", "Store_ID"])

        fact_sales_df["Trans_ID"] = fact_sales_df["Trans_ID"].astype(int)
        fact_sales_df["Store_ID"] = fact_sales_df["Store_ID"].astype(int)

        try:
            fact_sales_df.to_sql(
                "Fact_Sales",
                self.conn,
                if_exists="replace",
                index=False
            )
            print("Fact_Sales table loaded successfully")
            print("Rows loaded:", len(fact_sales_df))
        except Exception as e:
            print("Error while loading Fact_Sales table")
            print("Type      :", type(e).__name__)

    def load_dim_product(self):
        products = self.mysql_data["products"]
        product_df = pd.DataFrame(products)

        product_df = product_df[[
            "Product_ID",
            "Product_Name",
            "Unit_Price",
            "Unit_Cost",
            "SubCat_ID"
        ]]

        product_df = product_df.rename(columns={
            "Product_Name": "product_name",
            "Unit_Price": "unit_price",
            "Unit_Cost": "unit_cost",
            "SubCat_ID": "sub_cat_id"
        })

        # 🔹 Keep Product_ID as STRING
        product_df["Product_ID"] = product_df["Product_ID"].astype(str)

        # Convert only numeric measures
        product_df["unit_price"] = product_df["unit_price"].astype(float)
        product_df["unit_cost"] = product_df["unit_cost"].astype(float)
        product_df["sub_cat_id"] = pd.to_numeric(product_df["sub_cat_id"], errors="coerce")

        # --- Competitor price from web ---
        web_df = pd.DataFrame(self.web_data)
        web_df["norm_title"] = web_df["Title"].str.lower().str.strip()

        competitor_price_map = dict(
            zip(web_df["norm_title"], web_df["Price"])
        )

        product_df["norm_name"] = product_df["product_name"].str.lower().str.strip()
        product_df["competitor_price"] = product_df["norm_name"].map(competitor_price_map)

        # --- Sentiment ---
        sentiment_map = self.sentiment_analyzer
        product_df["sentiment_score"] = product_df["Product_ID"].map(sentiment_map)

        # Select final columns
        product_df = product_df[[
            "Product_ID",
            "product_name",
            "unit_price",
            "unit_cost",
            "sub_cat_id",
            "competitor_price",
            "sentiment_score"
        ]]

        # Fill optional attributes
        product_df["competitor_price"] = product_df["competitor_price"].fillna(0.0)
        product_df["sentiment_score"] = product_df["sentiment_score"].fillna(0.0)

        # 🔴 Drop products without Product_ID (business key)
        product_df = product_df.dropna(subset=["Product_ID"])

        # Convert decimals safely
        product_df = product_df.apply(lambda col: col.map(convert_decimal_to_float))

        try:
            product_df.to_sql(
                "Dim_Products",
                self.conn,
                if_exists="replace",
                index=False
            )
            print("Dim_Products table loaded successfully")
            print("Rows loaded:", len(product_df))
        except Exception as e:
            print("Error while loading Dim_Products table")
            print("Type      :", type(e).__name__)
            print("Message   :", str(e))

    def load_dim_customer(self):
        customers = self.mysql_data["customers"]
        cities = self.mysql_data["cities"]
        
        customers_df = pd.DataFrame(customers)
        cities_df = pd.DataFrame(cities)
        
        dim_customer_df = customers_df.merge(
            cities_df,
            on="City_ID",
            how="left"
        )
        
        dim_customer_df = dim_customer_df[[
            "Customer_ID",
            "Full_Name",
            "City_Name",
            "Region"
        ]]
        dim_customer_df = dim_customer_df.rename(columns={
            "Full_Name": "full_name",
            "City_Name": "city",
            "Region": "region"
        })
        dim_customer_df["city"] = dim_customer_df["city"].fillna("Unknown")
        dim_customer_df["region"] = dim_customer_df["region"].fillna("Unknown")

        try:
            dim_customer_df.to_sql(
                "Dim_Customer",
                self.conn,
                if_exists="replace",
                index=False
            )
            print("Dim_Customer table loaded successfully")
        except Exception as e:
            print("Error while loading Fact_Sales table")
            print("Type      :", type(e).__name__)
            print("Message   :", str(e))
            
    def load_dim_store(self):
        stores_df = pd.DataFrame(self.mysql_data["stores"])
        cities_df = pd.DataFrame(self.mysql_data["cities"])
        targets_df = self.excel_data["TARGETS_DF"].copy()

        dim_store = stores_df.merge(
            cities_df,
            on="City_ID",
            how="left"
        )
        targets_df["Month"] = pd.to_datetime(targets_df["Month"])
        latest_targets = (
            targets_df
            .sort_values("Month")
            .groupby("Store_ID", as_index=False)
            .last()[["Store_ID", "Target_Revenue"]]
            .rename(columns={"Target_Revenue": "sales_target"})
        )
        dim_store = dim_store.merge(
            latest_targets,
            on="Store_ID",
            how="left"
        )
        dim_store = dim_store[[
            "Store_ID",
            "Store_Name",
            "City_Name",
            "Region",
            "sales_target"
        ]].rename(columns={
            "Store_ID": "store_id",
            "Store_Name": "store_name",
            "City_Name": "city"
        })
        try:
            dim_store.to_sql(
                "Dim_Store",
                self.conn,
                if_exists="replace",
                index=False
            )
            print("Dim_Store table loaded successfully")
        except Exception as e:
            print("Error while loading Dim_Store table")
            print("Type      :", type(e).__name__)
            print("Message   :", str(e))

    def load_dim_date(self):
        
        date_series = []
        sales_df = pd.DataFrame(self.mysql_data["sales"])
        date_series.append(pd.to_datetime(sales_df["Date"]))
        targets_df = self.excel_data["TARGETS_DF"].copy()
        targets_df["Month"] = pd.to_datetime(targets_df["Month"])
        date_series.append(targets_df["Month"])
        all_dates = pd.concat(date_series)
        min_date = all_dates.min()
        max_date = all_dates.max()
        full_range = pd.date_range(start=min_date, end=max_date, freq="D")
        dim_date = pd.DataFrame({
            "full_date": full_range
        })

        dim_date["date_id"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["day"] = dim_date["full_date"].dt.day
        dim_date["month"] = dim_date["full_date"].dt.month
        dim_date["year"] = dim_date["full_date"].dt.year
        dim_date = dim_date[[
            "date_id",
            "full_date",
            "day",
            "month",
            "year"
        ]]
        try:
            dim_date.to_sql(
                "Dim_Date",
                self.conn,
                if_exists="replace",
                index=False
            )
            print("Dim_Date table loaded successfully")
        except Exception as e:
            print("Error while loading Dim_Date table")
            print("Type      :", type(e).__name__)
            print("Message   :", str(e))