import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://boughida.com/competitor/"

response = requests.get(url)
print("Status Code:", response.status_code)

soup = BeautifulSoup(response.text, "html.parser")
products = soup.find_all("div", class_="product")
len(products)



competitor_data = []

for product in products:
    name = product.find("h3").text.strip()
    price_text = product.find("span", class_="price").text.strip()
    
    price = float(price_text.replace("DZD", "").replace(",", "").strip())
    
    competitor_data.append({
        "Competitor_Product_Name": name,
        "Competitor_Price": price
    })

df_competitor_prices = pd.DataFrame(competitor_data)

df_competitor_prices.head()

df_competitor_prices.info()
df_competitor_prices.shape
