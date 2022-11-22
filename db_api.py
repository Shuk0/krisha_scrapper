import sqlite3 as sql

def write_parsed_data(offers_data: list):
    """ Function get parsed data in format list[(url_id, square, price, offer_date),...] and write into database"""
    
    # __check_data(offers_data)
    # Connect to database and get cursor
    try:
        conn = sql.connect('scrapper.db')
        with conn:
            cur = conn.cursor()

            while offers_data:  
                # Take one offer's data
                one_offer_data = offers_data.pop()
                # Check that data in offer is not None or 0
                if not all(one_offer_data): continue
                # Try to find information about this offer in database
                sql_answer_ad = __select_from_ad(conn, cur, (one_offer_data[0],))
                # If information about offer is in database                
                if sql_answer_ad:
                    sql_answer_price = __select_from_price(conn, cur, (sql_answer_ad[0],))
                    # If offer's date before than actual in DB, do nothing and take other offer
                    if one_offer_data[3] < sql_answer_price[0][2]: continue
                    else:
                        # So offer's date same or later than actaual in DB.
                        # If offer's price same than actual price in DB
                        if one_offer_data[2] == sql_answer_price[0][1]:
                            # If offer's date later than actual price in DB - update date
                            # else - do nothing
                            if one_offer_data[3] > sql_answer_price[0][2]:
                                data_for_update_date = (one_offer_data[3], sql_answer_ad[0], sql_answer_price[0][2])
                                __update_date_price(conn, cur, data_for_update_date)
                                
                            else: continue
                        # So offer's price and actual price in DB are different
                        else:
                            # Try to find offer's price from not actual in DB
                            new_price_in_db = None
                            for item in sql_answer_price:
                                if item[1] == one_offer_data[2]: 
                                    new_price_in_db = item
                                    break
                            # If there is offer's price in database
                            if new_price_in_db:
                                # If offer's date later than price's date in DB:
                                # update date and update cost 
                                if one_offer_data[3] > sql_answer_price[0][2]:
                                    data_for_update_date = (one_offer_data[3], sql_answer_ad[0], new_price_in_db[2])
                                    __update_date_price(conn, cur, data_for_update_date)
                                    data_for_update_ad = (round(float(one_offer_data[2]/one_offer_data[1]),2),
                                                            one_offer_data[0])
                                    __update_ad(conn, cur, data_for_update_ad)
                                    
                                # If offer's date same than price's date in DB:
                                # delete actual price, update date and update cost
                                else:
                                    data_for_del_price = (sql_answer_ad[0], sql_answer_price[0][1])
                                    __delete_price(conn, cur, data_for_del_price)
                                    data_for_update_date = (one_offer_data[3], sql_answer_ad[0], new_price_in_db[2])
                                    __update_date_price(conn, cur, data_for_update_date)
                                    data_for_update_ad = (round(float(one_offer_data[2]/one_offer_data[1]),2),
                                                            one_offer_data[0])
                                    __update_ad(conn, cur, data_for_update_ad)
                                    
                            #So there isn't offer's price in DB
                            else:
                                # If offer's date later than actual price's date:
                                # write new price and update cost 
                                if one_offer_data[3] > sql_answer_price[0][2]:
                                    data_for_insert_price = (one_offer_data[2], one_offer_data[3], sql_answer_ad[0])
                                    __insert_into_price(conn, cur, data_for_insert_price)
                                    data_for_update_ad = (round(float(one_offer_data[2]/one_offer_data[1]),2),
                                                            one_offer_data[0])
                                    __update_ad(conn, cur, data_for_update_ad)
                                    
                                # If offer's date same than actual price's date:
                                # update price and update cost
                                else:
                                    data_for_update_price = (one_offer_data[2], sql_answer_ad[0], sql_answer_price[0][2])
                                    __update_price(conn, cur, data_for_update_price)
                                    data_for_update_ad = (round(float(one_offer_data[2]/one_offer_data[1]),2),
                                                            one_offer_data[0])
                                    __update_ad(conn, cur, data_for_update_ad)                                                                                                        
                else:
                    # If information about concrete offer isn't in database -> write new information
                    data_for_insert_ad = (one_offer_data[0], one_offer_data[1],
                                            round(float(one_offer_data[2]/one_offer_data[1]),2))
                    ad_id = __insert_into_ad(conn, cur, data_for_insert_ad)
                    data_for_insert_price = (one_offer_data[2], one_offer_data[3], ad_id[0])
                    __insert_into_price(conn, cur, data_for_insert_price)

    except sql.Error as error:
            print("Can't connect to 'scrapper.db'. Error:", error.__class__, error)
    finally:
        cur.close()
        if conn:
            conn.close()
    
def __select_from_ad(conn: sql.Connection, cur: sql.Cursor, url_id: tuple) -> tuple:
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

def __insert_into_ad(conn: sql.Connection, cur: sql.Cursor, data_for_insert_ad: tuple) -> tuple:
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

def __update_ad(conn: sql.Connection, cur: sql.Cursor, data_for_update_ad: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (cost, url_id)
        and update cost into database
    """
    try:
        cur.execute('''UPDATE ad SET cost =? WHERE url_id = ?''', data_for_update_ad)
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

def __select_from_price(conn: sql.Connection, cur: sql.Cursor, ad_price: tuple) -> list:
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

def __insert_into_price(conn: sql.Connection, cur: sql.Cursor, data_for_insert_price: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (price, date, ad_price)
        and write information to database
    """
    try:
        cur.execute('''INSERT INTO price(price, date, ad_price) VALUES (?, ?, ?);''', data_for_insert_price)
        conn.commit()
    except sql.Error as err:
        __terminate_script(conn, cur, err)

def __update_date_price(conn: sql.Connection, cur: sql.Cursor, data_for_update_date: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (new_date, ad_price, old_date)
        and update date into database
    """
    try:
        cur.execute('''UPDATE price SET date = ? WHERE ad_price = ? AND date = ?''', data_for_update_date)
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

def __delete_price(conn: sql.Connection, cur: sql.Cursor, data_for_del_price: tuple):
    """ Function get sql.Connection, sql.Cursor and tuple (ad_price, price)
        and delete price from database
    """
    try:
        cur.execute('''DELETE FROM price WHERE ad_price = ? AND price = ?''', data_for_del_price)
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

def select_cost_ad():
    pass
