import requests
from bs4 import BeautifulSoup

page = requests.get('http://127.0.0.1:5500/krisha_scrapper/flat_list.html')
soup = BeautifulSoup(page.content, 'html.parser')

prices = soup.select("html body div.cnt-aprts-lst div.apartment div.price")
first5 = prices[:5]
for price in first5:
    print(price.text)