import requests
from bs4 import BeautifulSoup

def pagescrapper (url: str, page_number: int) -> str:
    #""" Function scrap site page by url and return html text"""

    page = requests.get(url.format(page_number))
    soup = BeautifulSoup(page.content, 'html.parser')

    return soup

def numberofoptins (soup: str) -> int:
    #'''Function parse scrapped text and return number of options'''
    
    text_number = soup.find('div', class_='a-search-subtitle search-results-nb').text.strip().split()
    number = int(text_number[1]) # number of options

    return number

def flatprice (soup: str, price: list) -> int:
    #'''Function parse scrapped text, change list price and return len of list price'''
    
    text_price = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                             class_='a-card__price')
    
    for each in text_price:
        text_price1 = ''.join(each.text.strip()[:-1].strip().split())
        price.append(int(text_price1))

        count = len(price)

    return count 

def squareofprice (soup: str, square: list) -> None:
    #'''Function parse scrapped text, change list square and return None'''

    text_square = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                             class_='a-card__header-left')

    for each in text_square:
        text_square1 = each.find('a', class_='a-card__title').text
        a = text_square1.find(',') 
        b = text_square1.rfind(',')
        square.append(float(text_square1[a+1:b].split()[0]))


    return None

def main():


    url = str('https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-zhetysu-1/?das[live.rooms]=1&page={}')
    page_number = int(1)
    count = int(0)
    number = int(1)
    price = []
    square = []
 

    while count < number:

        soup = pagescrapper(url, page_number)
        number = numberofoptins(soup)
        count = flatprice(soup, price)
        squareofprice(soup, square)

        if count < number: page_number = page_number + 1

    print(len(price))
    print(price)
    print(len(square))
    print(square)

if __name__ == '__main__':
    
    main()