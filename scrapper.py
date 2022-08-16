import requests
from bs4 import BeautifulSoup
from sys import argv
import re

def check_cli_arg(command_line_arguments: list) -> str:
    """ Function checks:
            - that URL was entered on command line, if not promts entering URL
            - URL starts with 'https://krisha.kz/', if not promts entering correct URL or exit
            and return URL
    """

    if len(command_line_arguments) == 1:
        cli_argument = input("Don't input URL. Input URL:")
    elif len(command_line_arguments) == 2:
        cli_argument = command_line_arguments[1]
    elif len(command_line_arguments) > 2:
        cli_argument = input('Too much arguments. Input URL:')

    while cli_argument.strip()[:18] != "https://krisha.kz/":
        cli_argument = input('Uncorrect URL. Try again or press ENTER to exit:')
        if cli_argument == "": exit(0)

    return cli_argument.strip()

def pagescrapper(accepted_url: str, page_number: int) -> str:
    """ Function scrap site page by url and return html text"""

    url = f'{accepted_url}&page={page_number}'
    
    page = requests.get(url, timeout=0.5)
    if page.status_code < 400:
        print(f'Page {page_number} scrapped. Status code: {page.status_code}', end = '\n')

    soup = BeautifulSoup(page.content, 'html.parser')

    return soup

def get_number_of_options(soup: str) -> int:
    """ Function parse scrapped text and return number of options"""
    
    text_with_numbers_of_options = soup.find('div',
                                     class_='a-search-subtitle search-results-nb').text.strip().split()
    numbers_of_options = int(text_with_numbers_of_options[1]) # number of options

    return numbers_of_options

def get_flat_price(soup: str, prices: list) -> int:
    """ Function parse scrapped text, add to list of prices new variants and return numbers_of_added_options"""
    
    text_with_prices = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                             class_='a-card__price')

    for each in text_with_prices:
        text_with_one_flat_price = ''.join(each.text.strip()[:-1].strip().split())
        prices.append(int(text_with_one_flat_price))

        numbers_of_added_options = len(prices)  # number of options added to list prices

    return numbers_of_added_options 

def get_flat_square(soup: str, squares: list) -> None:
    """ Function parse scrapped text, add to list of squares new variants and return None"""

    text_with_flats_square = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                                         class_='a-card__header-left')

    for each in text_with_flats_square:
        text_with_one_flat_square = each.find('a', class_='a-card__title').text
        indexes_of_square_number = re.search(r"\d+(\.\d*)?\sм", text_with_one_flat_square) 
        squares.append(float(text_with_one_flat_square[indexes_of_square_number.start():
                                                        indexes_of_square_number.end()-1]))

    return None

def get_offer_id(soup: str, id_list: list) -> None:
    """ Function parse scrapped text, add to list of offer id new variants and return None"""

    text_with_offers_id = soup.find('section', class_='a-list a-search-list a-list-with-favs').findAll('div',
                             class_='a-card__header-left')

    for each in text_with_offers_id:
        text_with_one_offer_id = each.find('a', class_='a-card__title').get('href')
        indexes_of_offer_id = re.search(r"\d+", text_with_one_offer_id)
        id_list.append(int(text_with_one_offer_id[indexes_of_offer_id.start():
                                                    indexes_of_offer_id.end()]))

    return None

def get_cost_per_sq_m(id_list: list, squares: list, prices: list) -> dict:
    """ Function get lists of id, squares and prices and return dictionary format {id : cost per m^2}"""
        
    cost_per_sq_m = {id_list[i]: int(prices[i]/squares[i]) for i in range(len(prices))}
    
    return cost_per_sq_m



def main():
    
# Use this argument for test e.g.: 
# https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-dostyk/?das[live.rooms]=1
# https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-zhetysu-1/?das[live.rooms]=1 

    command_line_arguments = argv #parse argument from CLI
    page_number = int(1)
    numbers_of_added_options = int(0)
    numbers_of_options = int(1)
    prices = []
    squares = []
    id_list = []
 
    accepted_url = check_cli_arg(command_line_arguments)

    while numbers_of_added_options < numbers_of_options:

        soup = pagescrapper(accepted_url, page_number)
        numbers_of_options = get_number_of_options(soup)
        numbers_of_added_options = get_flat_price(soup, prices)
        get_flat_square(soup, squares)
        get_offer_id(soup, id_list)

        if numbers_of_added_options < numbers_of_options: page_number = page_number + 1
    
    cost_per_sq_m = get_cost_per_sq_m(id_list, squares, prices)
    
    if len(prices) == len(squares) == len(id_list) == len(cost_per_sq_m) == numbers_of_options:
        print(f'Parsed and added {numbers_of_options} variants')
    else:
        print(f'Something wrong. Data is not correct', end = '\n')
        print(f'Number of options by site: {numbers_of_options}', end = '\n')
        print(f'Numbers of added prices: {len(prices)}', end = '\n')
        print(f'Numbers of added squares: {len(squares)}', end = '\n')
        print(f'Numbers of added id: {len(id_list)}', end = '\n')
        print(f'Numbers of added cost per sq_m: {len(cost_per_sq_m)}', end = '\n')
        print(id_list)
        print(prices)
        print(squares)
        print(cost_per_sq_m)
       
if __name__ == '__main__':
    
    main()