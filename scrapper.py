import argparse
import re
import requests
from bs4 import BeautifulSoup

def cli_parser() -> argparse.ArgumentParser:
    """ Function parse a command line. 
        Function format: python scrapper.py [-h] [url] [-ff file_with_url], where:
            - [-h] or [--help] optional argument. Description of ArgumentParser will be printed;
            - [url] positional argument site utl for scrapping;
            - [-ff file_with_url] or [--fromfile file_with_url] optional argument. PATH to file with site url.
        Function return ArgumentParser object. 
    """

    cli_parser = argparse.ArgumentParser(description='Scrapper from krisha.kz')
    cli_parser.add_argument('url', default=None, nargs='?',
                            help='site url for scrapping. Have to start with symbols "https://krisha.kz"'
                            )
    cli_parser.add_argument('-ff', '--fromfile',  dest='file_with_url',
                            help='''file with site url for scrapping.
                            You have to specify the full path to file 
                            or file have to be on the same folder'''
                            )
    args = cli_parser.parse_args()
    
    return [args.url, args.file_with_url]

def check_cli_arg(command_line_arguments: list) -> list:
    """ Function get command line arguments and check:
            - that URL or PATH to file with URL was entered in a command line;
            - that URL and PATH to file with URL didn't entered in a command line together.
    """

    if command_line_arguments[0] == None and command_line_arguments[1] == None:
        terminate_script(1)
    elif command_line_arguments[0] != None and command_line_arguments[1] != None:
        terminate_script(2)
    elif command_line_arguments[0] != None:
        return check_url(command_line_arguments[0])
    else:
        url = extract_url_from_file(command_line_arguments[1])
        return check_url(url)

def check_url(url:str) -> str:
    """Function check that URL starts with 'https://krisha.kz'"""

    if not re.search(re.compile('^https://krisha.kz'), url):
            print(url)
            terminate_script(3)
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
        terminate_script(4)

def terminate_script(exit_code: int) -> None:
    """ Function get exit code, print terminate message from dictionary according exit code
        and exit programm.
    """

    OUTPUT_MESSAGES = {
        1 : '''You missed an argument. It's exspected an URL or path to file with URL that should be grabbed. 
               Start the script with "-h" or "--help" optinal argument to get help''',
        2 : '''Too many arguments. It's exspected just one argument which URL or path to file with URL to be grabbed.
 	            Start the script with "-h" or "--help" optinal argument to get help''',
        3 : "Argument have to begin from 'https://krisha.kz'",
        4 : "File not found. Check the path and restart the script.",
        5 : "Page is not available. Check connection and restart the script."
        }
    
    print(OUTPUT_MESSAGES[exit_code])
    exit(1)

def grab_html_page(accepted_url: str, page_number: int) -> BeautifulSoup:
    """ Function scrap site page by url and return BeautifulSoup object."""

    url = f'{accepted_url}&page={page_number}'
    
    try:
        page = requests.get(url, timeout=0.5)
    except requests.ConnectionError:
        terminate_script(5)
    
    if page.status_code == 200:
        print(f'Page {page_number} scrapped. Status code: {page.status_code}', end = '\n')

    soup = BeautifulSoup(page.content, 'html.parser')

    return soup

def get_data(soup: BeautifulSoup, data: dict) -> int:
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
        text_with_one_flat_price = ''.join(each.find('div',
                                         class_='a-card__price').text.strip()[:-1].strip().split())
        price = int(text_with_one_flat_price)
        
        # parse flat's square
        text_with_one_flat_square = each.find('div',
                                         class_='a-card__header-left').find('a',
                                                                 class_='a-card__title').text
        # search start and end indexes of pattern like: "'digits' м"
        indexes_of_square_number = re.search(r"\d+(\.\d*)?\sм", text_with_one_flat_square) 
        square = float(text_with_one_flat_square[indexes_of_square_number.start():
                                                        indexes_of_square_number.end()-1])
        # parse offer id and url to offer
        text_with_one_offer_id = each.find('div',
                                     class_='a-card__header-left').find('a',
                                                                     class_='a-card__title').get('href')
        url_id = f'{URL}{text_with_one_offer_id}'
        # search  start and end indexes of digits in str
        indexes_of_offer_id = re.search(r"\d+", text_with_one_offer_id)
        id = int(text_with_one_offer_id[indexes_of_offer_id.start():
                                                    indexes_of_offer_id.end()])

        data[id] = (url_id, price, square)
        
    return len(data)

def get_number_of_options(soup: BeautifulSoup) -> int:
    """ Function parse scrapped BeautifulSoup object and return number of options."""
    
    content_of_div_with_number = soup.find('div',
                                     class_='a-search-subtitle search-results-nb').text.strip()
    try:
        numbers_of_options = int(content_of_div_with_number.encode("ascii", "ignore")) # number of options
    except TypeError:
        print("Can't parse number of options as extracted text is the following" + content_of_div_with_number)

    return numbers_of_options

def get_cost_per_sq_m(data: dict) -> dict:
    """ Function get dictinary with parsed data and return dictionary format {id : int(cost per m^2)}."""
        
    cost_per_sq_m = {key: int(data[key][1]/data[key][2]) for key in data.keys()}
    
    return cost_per_sq_m

def main():
    """ Function scrap site by URL which entered in command line.
        Result: dictinary with parsed data in format {offer id : tuple(url offer id, flat price, flat square)}
        and dictinary with cost per m^2 in format {id : int(cost per m^2)}.
    """
    
# Use this argument for test e.g.: 
# https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-dostyk/?das[live.rooms]=1
# https://krisha.kz/prodazha/kvartiry/almaty-aujezovskij-mkr-zhetysu-1/?das[live.rooms]=1 

    command_line_arguments = cli_parser()
    accepted_url = check_cli_arg(command_line_arguments)
    page_number = int(1)
    numbers_of_added_options = int(0)
    data = {}

    soup = grab_html_page(accepted_url, page_number)
    numbers_of_options = get_number_of_options(soup)

    # Scrapped and parsed site pages while numbers of parsed offers less than
    # numbers of offers stated by site
    while numbers_of_added_options < numbers_of_options:

        soup = grab_html_page(accepted_url, page_number)
        options_counter = len(data)
        numbers_of_added_options = get_data(soup, data)
        
        # There is a chance that an offer may be deleted during execution time of scrapper.py script.
        # In this case the last page may not include any offers. Hence, it's worth to check a number of offers on the last
        # page and if it's 0 than we need to invoke get_number_of_options function to update the variable numbers_of_options
        if options_counter == numbers_of_added_options: numbers_of_options = get_number_of_options(soup)
        if numbers_of_added_options < numbers_of_options: page_number = page_number + 1
    
    cost_per_sq_m = get_cost_per_sq_m(data)
    print(data, end='\n')
    print(cost_per_sq_m)

    if len(data) == numbers_of_options:
        print(f'Parsed and added {numbers_of_options} variants')
    else:
        print(f'Something wrong. Data is not correct', end = '\n')

       
if __name__ == '__main__':
    
    main()