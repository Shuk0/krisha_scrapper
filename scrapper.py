import argparse
import datetime
import re
import requests
from db_api import write_parsed_data
from bs4 import BeautifulSoup
from random import randint
from time import sleep

def parse_cli_arg() -> argparse.Namespace:
    """ Function parse a command line. 
        Function format: python scrapper.py [-h] [url] [-ff file_with_url], where:
            - [-h] or [--help] optional argument. Description of ArgumentParser will be printed;
            - [url] positional argument site utl for scrapping;
            - [-ff file_with_url] or [--fromfile file_with_url] optional argument. PATH to file with site url.
        Function return ArgumentParser object. 
    """

    cli_parser = argparse.ArgumentParser(description='Scrapper from krisha.kz')
    group = cli_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('url', default=None, nargs='?',
                            help='site url for scrapping. Have to start with symbols "https://krisha.kz"'
                        )
    group.add_argument('-ff', '--fromfile',  dest='file_with_url',
                        help='''file with site url for scrapping.
                             You have to specify the full path to file 
                            or file have to be on the same folder'''
                        )

    args = cli_parser.parse_args()
    
    return args

def check_cli_arg(command_line_arguments: list) -> list:
    """ Function get command line arguments and check:
            - that URL or PATH to file with URL was entered in a command line;
            - that URL and PATH to file with URL didn't entered in a command line together.
    """

    if command_line_arguments.url:
        return check_url(command_line_arguments.url)
    else:
        url = extract_url_from_file(command_line_arguments.file_with_url)
        return check_url(url)

def check_url(url:str) -> str:
    """Function check that URL starts with 'https://krisha.kz'"""

    if not re.search(re.compile('^https://krisha.kz'), url):
            terminate_script(1)
    else:
        return url

def extract_url_from_file(path:str) -> str:
    """Function get PATH to file with site url than read and return first line"""
    try:
        with open(path, 'r') as file:
            url = file.readline()
            url = url.rstrip('\n')
        return url
    except IOError:
        terminate_script(2)

def terminate_script(exit_code: int) -> None:
    """ Function get exit code, print terminate message from dictionary according exit code
        and exit programm.
    """

    OUTPUT_MESSAGES = {
        1 : "Argument have to begin from 'https://krisha.kz'",
        2 : "File not found. Check the path and restart the script.",
        3 : "Page is not available. Check connection and restart the script.",
        4 : "Page not answer too long. Check connection and restart the script."
        }
    
    print(OUTPUT_MESSAGES[exit_code])
    exit(1)

def grab_html_page(accepted_url: str, page_number: int) -> BeautifulSoup:
    """ Function scrap site page by url and return BeautifulSoup object."""

    url = f'{accepted_url}&page={page_number}'
    
    try:
        page = requests.get(url, timeout=0.5)
    except requests.ConnectionError:
        try:
            sleep(3)
            page = requests.get(url, timeout=0.5)
        except requests.ConnectionError:
            terminate_script(3)
    except requests.ReadTimeout:
        try:
            sleep(3)
            page = requests.get(url, timeout=0.5)
        except requests.ReadTimeout:
            terminate_script(4)
    
    if page.status_code == 200:
        print(f'Page {page_number} scrapped. Status code: {page.status_code}', end = '\n')

    soup = BeautifulSoup(page.content, 'html.parser')

    return soup

def get_data(soup: BeautifulSoup, offers_data: list) -> int:
    """ Function parse scrapped BeautifulSoup object, added new data in dictinary
        in format {offer id : tuple(url offer id, flat price, flat square)}
        and return lenth of dictinary.
    """

    URL = 'https://krisha.kz'
    # find all offers on page
    
    list_with_data = soup.find('section',
                            class_='a-list a-search-list a-list-with-favs').findAll('div',
                                                         class_=re.compile('^a-card a-storage-live ddl_product'))
    
    for each in list_with_data:
        # parse price of flat
        
        text_with_one_flat_price = each.find('div',
                                    class_='a-card__price').text
        list_with_one_flat_price = re.findall(r"\b[0-9]+", text_with_one_flat_price)
        try:
            price = int(''.join(list_with_one_flat_price))
        except ValueError or TypeError:
            # Need to add logging
            price = None
        
        # parse flat's square
        text_with_one_flat_square = each.find('div',
                                         class_='a-card__header-left').find('a',
                                                                 class_='a-card__title').text
        # search start and end indexes of pattern like: "'digits' м"
        try:
            indexes_of_square_number = re.search(r"\d+(\.\d*)?\sм", text_with_one_flat_square) 
            square = float(text_with_one_flat_square[indexes_of_square_number.start():
                                                        indexes_of_square_number.end()-1])
        except ValueError or TypeError:
            # Need to add logging
            square = None

        # parse offer id and url to offer
        try:
            text_with_one_offer_id = each.find('div',
                                     class_='a-card__header-left').find('a',
                                                                     class_='a-card__title').get('href')
        
            #url_id = f'{URL}{text_with_one_offer_id}'
            url_id = str(text_with_one_offer_id)
        except ValueError or TypeError:
            # Need to add logging
            url_id = None

        # search start and end indexes of digits in str
        # try:
        #     indexes_of_offer_id = re.search(r"\d+", text_with_one_offer_id)
        #     id = int(text_with_one_offer_id[indexes_of_offer_id.start():
        #                                             indexes_of_offer_id.end()])
        # except ValueError or TypeError:
        #     # Need to add logging
        #     id = text_with_one_offer_id

        # search a date of offer

        text_with_offer_date = each.find('div', class_='card-stats').text
        offer_date = re.search(r"[0-9]+\s([А-Яа-я])+\.", text_with_offer_date).group().split()
        offer_date = convert_date(offer_date)

        offers_data.append((url_id, square, price, offer_date))
        
    return len(offers_data)

def get_number_of_options(soup: BeautifulSoup) -> int:
    """ Function parse scrapped BeautifulSoup object and return number of options."""
    
    content_of_div_with_number = soup.find('div',
                                     class_='a-search-subtitle search-results-nb').text.strip()
    try:
        numbers_of_options = int(content_of_div_with_number.encode("ascii", "ignore")) # number of options
    except TypeError:
        print("Can't parse number of options as extracted text is the following" + content_of_div_with_number)

    return numbers_of_options

def convert_date(offer_date: list) -> str:
    """ Function convert date from format e.g. [DD, 'янв.'] to ISO 8601 format: YYYY-MM-DD"""

    month_dict = {
        'янв.' : 1,
        'фев.' : 2,
        'мар.' : 3,
        'апр.' : 4,
        'май' : 5,
        'июн.' : 6,
        'июл.' : 7,
        'авг.' : 8,
        'сен.' : 9,
        'окт.' : 10,
        'нояб.' : 11,
        'дек.' : 12,
    }
    
    month = month_dict[offer_date[1]]
    year = datetime.date.today().year
    offer_date = datetime.date(year, month, int(offer_date[0])).isoformat()
    return offer_date

def main():
    """ Function scrap site by URL which entered in command line.
        Result: dictinary with parsed data in format {offer id : tuple(url offer id, flat price, flat square)}
        and dictinary with cost per m^2 in format {id : int(cost per m^2)}.
    """
    
# Use this argument for test e.g.: 
# https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-dostyk/?das[live.rooms]=1
# https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-zhetysu-1/?das[live.rooms]=1 

    command_line_arguments = parse_cli_arg()
    accepted_url = check_cli_arg(command_line_arguments)
    page_number = int(1)
    numbers_of_parsed_options = int(0)
    offers_data = []

    soup = grab_html_page(accepted_url, page_number)
    numbers_of_options = get_number_of_options(soup)

    # Scrapped and parsed site pages while numbers of parsed offers less than
    # numbers of offers stated by site
    while numbers_of_parsed_options < numbers_of_options:

        soup = grab_html_page(accepted_url, page_number)
        options_counter = numbers_of_parsed_options
        numbers_of_parsed_options =  numbers_of_parsed_options + get_data(soup, offers_data)
        write_parsed_data(offers_data)
        sleep_time = randint(4, 8)
        sleep(sleep_time)
        # There is a chance that an offer may be deleted during execution time of scrapper.py script.
        # In this case the last page may not include any offers. Hence, it's worth to check a number of offers on the last
        # page and if it's 0 than we need to invoke get_number_of_options function to update the variable numbers_of_options
        if options_counter == numbers_of_parsed_options: numbers_of_options = get_number_of_options(soup)
        if options_counter == numbers_of_options: break #
        if numbers_of_parsed_options < numbers_of_options: page_number = page_number + 1

    if numbers_of_parsed_options == numbers_of_options:
        print(f'Parsed and added {numbers_of_options} variants')
    else:
        print(f'Something wrong. Data is not correct', end = '\n')

       
if __name__ == '__main__':
    
    main()
