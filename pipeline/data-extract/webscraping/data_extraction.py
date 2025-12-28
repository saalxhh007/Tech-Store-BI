import requests
import random
from bs4 import BeautifulSoup
from pprint import pprint

products = []

url = "https://boughida.com/competitor/"
try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print("Request failed:", e)
    exit()

if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")
    product_cards = soup.select("div.product-card")
    # print(product_cards)

    for product_card in product_cards:
        title = product_card.find("h5")
        id = product_card.find("p")
        price = product_card.find("span", class_="product-price")
        old_price = product_card.find("span", class_="old-price") if product_card.find("span", class_="price-tag") else None
        price = product_card.find("span", class_="product-price")
        products.append({  
            "Title": title.get_text(strip=True) if title else "",
            "ID": id.get_text(strip=True) if price else "",
            "Price": price.get_text(strip=True) if price else "",
            "Old Price": old_price.get_text(strip=True) if old_price else ""
        })

pprint(random.sample(products, 10))