import numpy as np
import sqlite3 as sql
from collections import namedtuple
from functools import wraps

class Outliers_in():
    """ Dataclass with query information about outliers. Include 3 bool arguments:
        - outliers - Do you need information about outliers. Default: False;
        - higher - Do you need information about outliers higher than data. Default: True;
        - lower - Do you need information about outliers lower than data. Default: True.
    """
    def __init__(self, outliers: bool = False, higher: bool = True, lower: bool = True):
        self.outliers = outliers
        self.higher = higher
        self.lower = lower

class Percentile_in():
    """ Dataclass with query information about percentile. Include next arguments:
        - perc_number - Int typed percentile number. Default: 0;
        - higher - Bool typed argument. Do you need information higher than percentile bound. Default: True;
    """
    def __init__(self, perc_number: int = 0, perc_higher: bool = True):
        self.perc_number = perc_number
        self.perc_higher = perc_higher

class Histogram_in():
    """ Dataclass with query information about histogram. Include 3 arguments:
        - histogram - Bool typed argument. Do you need information about histogram. Default: False;
        - with_outliers - Bool typed argument. Do you need information with_outliers. Default: False;
        - lower - Int typed argument. Number of equal-width bins. Default: 10.
    """
    def __init__(self, histogram: bool = False, with_outliers: bool = False, bins: int = 10):
        self.histogram = histogram
        self.with_outliers = with_outliers
        self.bins = bins

class Query_in():
    """ Dataclass with all query information. Include next arguments:
        - min, max, avg, median - Bool typed argument.
                                    Do you need information about min, max, avg, median. Default: False;
        - offer_info - Str typed argument. Do you need actual information about offer.
                        Include offer's url_id. Default: None;
        - offer_history - Str typed argument. Do you need history information about offer. 
                        Include offer's url_id. Default: None;
        - outliers - 'Outliers_in' typed argument. More information in class Outliers_in;
        - percentile - 'Percentile_in' typed argument. More information in class Percentile_in;
        - histogram - 'Histogram_in' typed argument. More information in class Histogram_in.
    """
    def __init__(self, min: bool = False, max: bool= False,
                 avg: bool = False, median: bool = False,
                 offer_info: str = None, offer_history: str = None,
                 outliers: Outliers_in = (False, True, True), percentile: Percentile_in = (0, True),
                 histogram: Histogram_in = (False, False, 10)):
        self.min = min
        self.max = max
        self.avg = avg
        self.median = median
        self.offer_info = offer_info
        self.offer_history = offer_history
        self.outliers = Outliers_in(*outliers)
        self.percentile = Percentile_in(*percentile)
        self.histogram = Histogram_in(*histogram)

Offer = namedtuple("Offer", "offer_url price date cost square")
Price = namedtuple("Price", "price date")
Outliers_out = namedtuple("Outliers_out", "low_bound high_bound outliers_min outliers_max")
Histogram_out = namedtuple("Histogram_out", "hist bin_edges")
Query_out = namedtuple("Query_out", "min max avg median offer_info offer_history outliers percentile histogram")

class Query_out(Query_out):
    def __str__(self):
        return f'''Query_out:\n\tMin: {self.min}\tMax: {self.max}\n\tAvg: {self.avg}\tMedian: {self.median}\n
        Offer_info\n{self.offer_info}\nOffer_history\n{self.offer_history}\n
        Outliers\n{self.outliers}\nPercentile\n{self.percentile}\nHistogram\n{self.histogram}'''

def listfactory(cur: sql.Cursor, row: sql.Row):    #move to db_api.py
    return row[0]

def namedtuplefactory(cursor: sql.Cursor, row: sql.Row): #move to db_api.py

    fields = [column[0] for column in cursor.description]
    match fields:
        case "url_id", "price", "date", "cost", "square":
            name = "Offer"
        case "price", "date":
            name = "Price"
    cls = namedtuple(name, fields)
    return cls._make(row)

def __is_argument_type_Query_in(function):
    """ Check that argument type is 'Query_in'"""
    def wrapper(arg):
        if not isinstance(arg, Query_in):
            print(f'Error: Expected {Query_in}. Recieved: {type(arg)}')
            exit(1)
        result = function(arg)
        return result
    return wrapper

@__is_argument_type_Query_in
def query(query_params: Query_in) -> Query_out:
    """ Function get query as 'Query_in' typed data.
        Calculate answer used numpy module and database through db_api.py module
        and return informatin as namedtuple 'Query_out'.
    """
    low_bound, high_bound = None, None
    list_of_costs = []

    if query_params.min: low_bound, high_bound, list_of_costs, ans_min  = minimal(low_bound, high_bound, list_of_costs)
    else: ans_min = None
    if query_params.max: low_bound, high_bound, list_of_costs, ans_max = maximal(low_bound, high_bound, list_of_costs)
    else: ans_max = None
    if query_params.avg: low_bound, high_bound, list_of_costs, ans_avg = average(low_bound, high_bound, list_of_costs)
    else: ans_avg = None
    if query_params.median: low_bound, high_bound, list_of_costs, ans_median\
                             = median_func(low_bound, high_bound, list_of_costs)
    else: ans_median = None
    if query_params.offer_info: ans_offer_info = tuple(select("offer_info", query_params.offer_info))
    else: ans_offer_info = None
    if query_params.offer_history: ans_offer_history = tuple(select("offer_history", query_params.offer_history))
    else: ans_offer_history = None
    if query_params.outliers.outliers:
        low_bound, high_bound, outliers_min, outliers_max = outliers_func(low_bound, high_bound, query_params.outliers)
    else: outliers_min, outliers_max = None, None
    if query_params.percentile.perc_number:
        low_bound, high_bound, list_of_costs, perc_bound, list_of_offers\
            = percentile_func(low_bound, high_bound, list_of_costs, query_params.percentile)
    else: perc_bound, list_of_offers = None, None
    if query_params.histogram.histogram:
        ans_hist, ans_bin = histogram_func(low_bound, high_bound, list_of_costs, query_params.histogram)
    else: ans_hist, ans_bin = None, None
    outliers = Outliers_out(low_bound, high_bound, outliers_min, outliers_max)
    percentile = (perc_bound, list_of_offers)
    histogram = Histogram_out(ans_hist, ans_bin)
    answer = Query_out(ans_min, ans_max, ans_avg, ans_median, ans_offer_info, ans_offer_history,\
                       outliers, percentile, histogram)
    return answer

def __math(func):
    @wraps(func)
    def wrapper(*args):
        if not args[0] and not args[1]:
            (low_bound, high_bound) = bounds_outliers()
        else:
            low_bound, high_bound = args[0], args[1]
        if args[2] == []:
            list_of_costs = select("cost_between", low_bound, high_bound)
        else:
            list_of_costs = args[2]

        func_dict = {
                    "minimal": np.min(list_of_costs),
                    "maximal": np.max(list_of_costs),
                    "average": round(np.mean(list_of_costs), 2),
                    "median_func": round(np.median(list_of_costs), 2),
                    }
        
        return (low_bound, high_bound, list_of_costs, func_dict[func.__name__])
        
    return wrapper

@__math
def minimal(low_bound: float = None, high_bound: float = None, list_of_costs: list = None) -> tuple:
    """ Function:
        1) call to database used db_apy.py to get costs
        2) find minimal cost used 'min' function from numpy
        and return it as float rounded 2nd decimal place.
    """
    pass

@__math
def maximal(low_bound: float = None, high_bound: float = None, list_of_costs: list = None) -> tuple:
    """ Function:
        1) call to database used db_apy.py to get costs
        2) find maximal cost used 'max' function from numpy
        and return it as float rounded 2nd decimal place.
    """
    pass

@__math
def average(low_bound: float = None, high_bound: float = None, list_of_costs: list = None) -> tuple:
    """ Function:
        1) call to database used db_apy.py to get costs
        2) compute average of costs used 'mean' function from numpy
        and return it as float.
    """ 
    pass

@__math
def median_func(low_bound: float = None, high_bound: float = None, list_of_costs: list = None) -> tuple:
    """ Function:
        1) call to database used db_apy.py to get costs
        2) compute median of costs used 'median' function from numpy
        and return it as float rounded 2nd decimal place.
    """
    pass

def percentile_func(low_bound: float = None, high_bound: float = None, list_of_costs: list = None,
                     percentile: Percentile_in = None) -> tuple:
    """ Function get percentil's number and:
        1) call to database used db_apy.py to get costs
        2) compute percentil bound used 'percentile' function from numpy
        and return percentil bound and tuple of offers.
    """
    
    if not low_bound and not high_bound:
        (low_bound, high_bound) = bounds_outliers()

    if list_of_costs == []:
        list_of_costs = select("cost_between", low_bound, high_bound)

    perc_bound = round(np.percentile(list_of_costs, percentile.perc_number), 2)

    match percentile.perc_higher:
        case True:
            list_of_offers = select("offers_info_higher", perc_bound)
        case False:
            list_of_offers = select("offers_info_lower", perc_bound)

    return low_bound, high_bound, list_of_costs, perc_bound, tuple(list_of_offers)

def outliers_func(low_bound:float, high_bound:float, outliers: Outliers_in) -> tuple:
    """ Function call to database used db_apy.py to get outliers offers and return them.
    """
    if not low_bound and not high_bound: 
        (low_bound, high_bound) = bounds_outliers()
    if outliers.higher and high_bound:
        outliers_max = select("offers_info_higher", high_bound)
    else: outliers_max = None
    if outliers.lower and low_bound:
        outliers_min = select("offers_info_lower", low_bound)
    else: outliers_min = None

    return low_bound, high_bound, outliers_min, outliers_max

def histogram_func(low_bound:float, high_bound:float, list_of_costs: list, query_params: Histogram_in) -> tuple:
    """ Function:
        1) call to database used db_apy.py to get costs
        2) compute tuple of values and tuple of bin edges used 'histogram' function from numpy
        and return tuple of values and tuple of bin edges.
    """

    if query_params.with_outliers:
        list_of_all_costs = select("all_cost")
        ans_hist = np.histogram(list_of_all_costs, bins = query_params.bins)
    else:
        if list_of_costs == [] and not low_bound and not high_bound:
            (low_bound, high_bound) = bounds_outliers()
            list_of_costs = select("cost_between", low_bound, high_bound)

        ans_hist, ans_bin = np.histogram(list_of_costs, bins = query_params.bins)
        ans_hist = tuple(ans_hist)
        ans_bin = tuple(ans_bin)

    return ans_hist, ans_bin

def bounds_outliers():
    """ Function:
        1) call to database used db_apy.py to get costs
        2) compute low nad high bounds
        and return them as tuple of floats rounded 2nd decimal place i.e., (low_bound, high_bound).

        low bound calculate as 25th percentile - 1,5*(75th percentile - 25th percentile),
        high bound calculate as 75th percentile + 1,5*(75th percentile - 25th percentile).
        Function used 'percentile' function from numpy.
    """
    list_of_costs = select("all_cost")
    perc25 = round(np.percentile(list_of_costs, 25), 2)
    perc75 = round(np.percentile(list_of_costs, 75), 2)

    return (round(perc25 - 1.5*(perc75 - perc25), 2), round(perc75 + 1.5*(perc75 - perc25), 2))

def select(query, *args) -> list:                   # move to db_api.py
    """ Function connect to database used db_api.py module
        and return answer.
    """
    try:
        conn = sql.connect('scrapper.db')
    
        match query:
            case "all_cost": 
                sql_query = "SELECT cost FROM ad;"
                conn.row_factory = listfactory
            case "cost_between": 
                sql_query = "SELECT cost FROM ad WHERE cost BETWEEN ? AND ?;"
                conn.row_factory = listfactory
            case "offer_info": 
                sql_query = """SELECT url_id, price, date, cost, square FROM ad
                                INNER JOIN price ON ad.ad_id = price.ad_price WHERE url_id= ?
                                ORDER BY date DESC;"""
                conn.row_factory = namedtuplefactory
            case "offer_history": 
                sql_query = """SELECT price, date FROM ad
                                INNER JOIN price ON ad.ad_id = price.ad_price WHERE url_id= ?
                                ORDER BY date DESC;"""
                conn.row_factory = namedtuplefactory
            case "offers_info_higher": 
                sql_query = """SELECT url_id, price, date, cost, square FROM ad
                                INNER JOIN price ON ad.ad_id = price.ad_price WHERE cost>= ?
                                ORDER BY cost;"""
                conn.row_factory = namedtuplefactory
            case "offers_info_lower": 
                sql_query = """SELECT url_id, price, date, cost, square FROM ad
                                INNER JOIN price ON ad.ad_id = price.ad_price WHERE cost< ?
                                ORDER BY cost;"""
                conn.row_factory = namedtuplefactory

        cur = conn.cursor()
        cur.execute(sql_query, (args))
        sql_answer = cur.fetchall()
        conn.commit()
    except sql.Error as error:
        print("Can't connect to 'scrapper.db'. Error:", error.__class__, error)
        exit(1)
        
    return sql_answer

def main():

    query_in = Query_in(min= True, max= True, avg= True, median= True, offer_info= '/a/show/680849283',\
                        offer_history= '/a/show/680849283', outliers= (True, True, True),\
                        percentile= (90, True), histogram= (True, False, 10))
    ans = query(query_in)
    print(ans)

       
if __name__ == '__main__':
    main()

# def variance(ddof=1) -> float:
#     ''' Function get optional parameter 'ddof'(by default, ddof = 1) and:
#         1) call to database used db_apy.py to get costs
#         2) compute variance of cost's used 'var' function from numpy
#         and return it as float rounded 2nd decimal place.
#         Variance is the average of the squared deviations from the mean,
#          i.e., variance = mean(x), where x = abs(a - a.mean())**2
#         The mean is calculated as x.sum() / (N - ddof), where N = len(x).
#     '''
#     (low_bound, high_bound) = bounds_outliers()
#     list_of_costs = select_cost(conn, cur, "cost_between", low_bound, high_bound) 
    
#     return round(np.var(list_of_costs, ddof), 2)

# def stdev(ddof=1) -> float:
#     ''' Function get optional parameter 'ddof'(by default, ddof = 1) and:
#         1) call to database used db_apy.py to get costs
#         2) compute standard deviation of costs used 'std' function from numpy 
#         and return it as float rounded 2nd decimal place.
        
#         Standard deviation is s the square root of the squared deviations from the mean, 
#         i.e., stdev = sqrt(mean(x)), where x = abs(a - a.mean())**2
#         The mean is calculated as x.sum() / (N - ddof), where N = len(x).
#     '''
#     (low_bound, high_bound) = bounds_outliers()
#     list_of_costs = select_cost(conn, cur, "cost_between", low_bound, high_bound) 
    
#     return round(np.std(list_of_costs, ddof), 2)