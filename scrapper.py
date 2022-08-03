import requests
from bs4 import BeautifulSoup

url = 'https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-zhetysu-1/?das[live.rooms]=1&page={}'

count = int(0)
page_number = 1

page = requests.get(url.format(page_number))
soup = BeautifulSoup(page.content, 'html.parser')

text_number = soup.find('div', class_='a-search-subtitle search-results-nb').text.strip().split()
number = int(text_number[1]) # number of options

while count < number:
    text_price = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                             class_='a-card__price')
    if count == 0: price = []
    for each in text_price:
        text_price1 = ''.join(each.text.strip()[:-1].strip().split())
        price.append(int(text_price1))

    text_square = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                             class_='a-card__header-left')

    if count == 0: s = []
    for each in text_square:
        text_s1 = each.find('a', class_='a-card__title').text
        a = text_s1.find(',')
        b = text_s1.rfind(',')
        s.append(float(text_s1[a+1:b].split()[0]))
    
    count = len(price)
    if count < number:
        page_number+=1
        page = requests.get(url.format(page_number))
        soup = BeautifulSoup(page.content, 'html.parser')

#try len(price) == len(s) == number # Test: we have to get same numbers of data

print(len(price))
print(price)
print(len(s))
print(s)