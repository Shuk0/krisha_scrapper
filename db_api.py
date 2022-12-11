import sqlite3 as sql

def write_parsed_data(offers_data: list):
    """ Function get parsed data in format list[(url_id, square, price, offer_date),...] and write into database"""
    
    # Connect to database and get cursor
    try:
        conn = sql.connect('scrapper.db')
        with conn:
            cur = conn.cursor()

            while offers_data:  
                # Take one offer's data
                one_offer_data = offers_data.pop()

                # Check that data in offer is not None or 0
                if __is_offer_data_include_None_or_0(one_offer_data): continue
              
                # Try to find information about this offer in database
                sql_answer_ad = __select_from_ad_table(conn, cur, (one_offer_data[0],))

                # If information about offer is in database                
                if sql_answer_ad:
                    # Get actual price and date from database
                    sql_answer_price = __select_from_price_table(conn, cur, (sql_answer_ad[0],))
              
                    # If offer's price not equal actual price in database and
                    # offer's date equal actual(later) date in database: update price and cost in database 
                    if (__is_offer_price_equal_actual_price_in_db(one_offer_data[2], sql_answer_price[0][1], "=") == False and 
                        __is_offer_date_equal_actual_date_in_db(one_offer_data[3], sql_answer_price[0][2], "=") == True):
                        data_for_update_price = (one_offer_data[2], sql_answer_ad[0], sql_answer_price[0][2])
                        __update_price(conn, cur, data_for_update_price)
                        data_for_update_ad = (round(float(one_offer_data[2]/one_offer_data[1]),2),
                                                one_offer_data[0])
                        __update_cost(conn, cur, data_for_update_ad)

                    # If offer's price not equal actual price in database and
                    # offer's date later than actual(later) date in database: write new price and update cost in database
                    if (__is_offer_price_equal_actual_price_in_db(one_offer_data[2], sql_answer_price[0][1], "=") == False and 
                            __is_offer_date_later_than_actual_date_in_db(one_offer_data[3], sql_answer_price[0][2], ">") == True):
                        data_for_insert_price = (one_offer_data[2], one_offer_data[3], sql_answer_ad[0])
                        __insert_into_price_table(conn, cur, data_for_insert_price)
                        data_for_update_ad = (round(float(one_offer_data[2]/one_offer_data[1]),2),
                                                one_offer_data[0])
                        __update_cost(conn, cur, data_for_update_ad)

                else:
                    # If information about concrete offer isn't in database -> write new information
                    data_for_insert_ad = (one_offer_data[0], one_offer_data[1],
                                            round(float(one_offer_data[2]/one_offer_data[1]),2))
                    ad_id = __insert_into_ad_table(conn, cur, data_for_insert_ad)
                    data_for_insert_price = (one_offer_data[2], one_offer_data[3], ad_id[0])
                    __insert_into_price_table(conn, cur, data_for_insert_price)

    except sql.Error as error:
            print("Can't connect to 'scrapper.db'. Error:", error.__class__, error)
    finally:
        cur.close()
        if conn:
            conn.close()
    
def __select_from_ad_table(conn: sql.Connection, cur: sql.Cursor, url_id: tuple) -> tuple:
    """ Function get sql.Connection, sql.Cursor and tuple (url_id, )
        and return tuple (ad_id, url_id, square, cost) or None
    """
    try:
        cur.execute('''SELECT ad_id, url_id, square, cost FROM ad WHERE url_id= ?;''', url_id)
        sql_answer = cur.fetchone()
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)
        
    return sql_answer

def __insert_into_ad_table(conn: sql.Connection, cur: sql.Cursor, data_for_insert_ad: tuple) -> tuple:
    """ Function get sql.Connection, sql.Cursor and tuple (url_id, square, cost)
        and write information into database. Return tuple with ad_id.
    """
    try:
        cur.execute('''INSERT INTO ad(url_id, square, cost) VALUES (?, ?, ?) RETURNING ad_id;''', data_for_insert_ad)
        ad_id = cur.fetchone()
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

    return ad_id

def __update_cost(conn: sql.Connection, cur: sql.Cursor, data_for_update_ad: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (cost, url_id)
        and update cost into database
    """
    try:
        cur.execute('''UPDATE ad SET cost =? WHERE url_id = ?''', data_for_update_ad)
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

def __select_from_price_table(conn: sql.Connection, cur: sql.Cursor, ad_price: tuple) -> list:
    """ Function get sql.Connection, sql.Cursor and tuple (ad_price, )
        and return list of tuple (ad_price, price, date)
    """
    try:
        cur.execute('''SELECT ad_price, price, date FROM price WHERE ad_price= ? ORDER BY date DESC;''', ad_price)
        sql_answer = cur.fetchall()
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

    return sql_answer

def __insert_into_price_table(conn: sql.Connection, cur: sql.Cursor, data_for_insert_price: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (price, date, ad_price)
        and write information to database
    """
    try:
        cur.execute('''INSERT INTO price(price, date, ad_price) VALUES (?, ?, ?);''', data_for_insert_price)
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

def __update_price(conn: sql.Connection, cur: sql.Cursor, data_for_update_price) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (new_price, ad_price, date)
        and update date into database
    """
    try:
        cur.execute('''UPDATE price SET price = ? WHERE ad_price = ? AND date = ?''', data_for_update_price)
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

def __terminate_script(conn: sql.Connection, cur: sql.Cursor, err:sql.Error) -> None:
    """ Function get sql.Connection, sql.Cursor, sql.Error, print err.__class__ and err and exit programm.
    """
    print('Error:', err.__class__, err)
    cur.close()
    if conn:
        conn.close()
    exit(0)

def select_cost(conn: sql.Connection, cur: sql.Cursor) -> list:
    """ Function get sql.Connection, sql.Cursor 
        and return list of tuples (url_id, cost) or []
    """
    try:
        cur.execute('''SELECT url_id, cost FROM ad;''')
        sql_answer = cur.fetchall()
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)
        
    return sql_answer

def __is_offer_data_include_None_or_0(one_offer_data:tuple) -> bool:
    """ Function get tuple (url_id, square, price, offer_date) and 
        return True if any element of tuple is None or 0
        otherwise return False
    """
    return not all(one_offer_data)

def comparison(function):
    """ Function get two float arguments and type of comparison as str
        Varians of type of comparison:
            "=" : equal;
            ">" : bigger;
            "<" : smaller.
        Function return result of comparision type bool. 
        (e.g. arguments are a: float, b: float, "=". So we return result of a == b) 
    """
    def wrapper(*args, **kwargs):

        def equal(*args, **kwargs) -> bool:
            return args[0] == args[1]
        
        def bigger(*args, **kwargs) -> bool:
            return args[0] > args[1]

        def smaller(*args, **kwargs) -> bool:
            return args[0] < args[1]
            
        func_dict = {
        "=" : equal(*args, **kwargs),
        ">" : bigger(*args, **kwargs),
        "<" : smaller(*args, **kwargs)
        }

        return func_dict[args[2]]

    return wrapper

@comparison
def __is_offer_price_equal_actual_price_in_db(first_element: float, second_element: float, operator: str) -> None:
    """ Function give all recieved arguments to wrapper "comparison" and do nothing
    """
    pass

@comparison
def __is_offer_date_equal_actual_date_in_db(first_element: float, second_element: float, operator: str) -> None:
    """ Function give all recieved arguments to wrapper "comparison" and do nothing
    """
    pass

@comparison
def __is_offer_date_later_than_actual_date_in_db(first_element: float, second_element: float, operator: str) -> None:
    """ Function give all recieved arguments to wrapper "comparison" and do nothing
    """
    pass

