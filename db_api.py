import sqlite3 as sql
import datetime




def write_parsed_data(offers_data: list):
    """ Function write parsed data to database"""
    
    try:
        conn = sql.connect('scrapper.db')
    except sql.Error as error:
            print("Can't connect to 'scrapper.db'. Error:", error)
    
    cur = conn.cursor()

    while offers_data:
        # Take one offer's data
        one_offer_data = offers_data.pop()
        # Try to find information about this offer in database
        sql_answer_ad = __select_from_ad(conn, cur, (one_offer_data[0],))
        if sql_answer_ad:
            # If information about concrete offer is in database -> compare price from DB and new one
            sql_answer_price = __select_from_price(conn, cur, (sql_answer_ad[0],))
            if sql_answer_price[0] == one_offer_data[2]: pass
            else:
                # If prices are different, write new information
                one_offer_data_for_price = (one_offer_data[2], one_offer_data[3], sql_answer_ad[0])
                __insert_into_price(conn, cur, one_offer_data_for_price)
                one_offer_data_for_sq_per_m = (float(one_offer_data[2]/one_offer_data[1]), sql_answer_ad[0])
                __update_sq_per_m(conn, cur, one_offer_data_for_sq_per_m)
        else:
            # If information about concrete offer isn't in database -> write new information
            one_offer_data_for_ad = (one_offer_data[0], one_offer_data[1])
            ad_id = __insert_into_ad(conn, cur, one_offer_data_for_ad)
            one_offer_data_for_price = (one_offer_data[2], one_offer_data[3], ad_id[0])
            __insert_into_price(conn, cur, one_offer_data_for_price)
            one_offer_data_for_sq_per_m = (float(one_offer_data[2]/one_offer_data[1]), ad_id[0])
            __insert_into_sq_per_m(conn, cur, one_offer_data_for_sq_per_m)

    conn.commit()
    cur.close()
    conn.close()
    
def __select_from_ad(conn: sql.Connection, cur: sql.Cursor, url_id: tuple) -> tuple:
    """ Function get sql.Connection, sql.Cursor and tuple (url_id, )
        and return tuple (ad_id, krisha_id, square) or None
    """
    try:
        cur.execute('''SELECT ad_id, krisha_id, square FROM ad WHERE krisha_id= ?;''', url_id)
        sql_answer = cur.fetchone()
    except sql.OperationalError as err:
            print('Error:', err.__class__, err)
            cur.close()
            conn.close()
            exit(0)
    return sql_answer

def __select_from_price(conn: sql.Connection, cur: sql.Cursor, ad_price: tuple) -> tuple:
    """ Function get sql.Connection, sql.Cursor and tuple (ad_price, )
        and return tuple (price, MAX(date), ad_price) or None
    """
    try:
        cur.execute('''SELECT price, MAX(date), ad_price FROM price WHERE ad_price= ? GROUP BY ad_price;''', ad_price)
        sql_answer = cur.fetchone()
    except sql.OperationalError as err:
        print('Error:', err.__class__, err)
        cur.close()
        conn.close()
        exit(0)
    return sql_answer

def __insert_into_ad(conn: sql.Connection, cur: sql.Cursor, one_offer_data_for_ad: tuple) -> tuple:
    """ Function get sql.Connection, sql.Cursor and tuple (krisha_id, square)
        and write information to database. Return tuple ad_id.
    """
    try:
        cur.execute('''INSERT INTO ad(krisha_id, square) VALUES (?, ?) RETURNING ad_id;''', one_offer_data_for_ad)
        ad_id = cur.fetchone()[0]
    except sql.OperationalError as err:
        print('Error:', err.__class__, err)
        cur.close()
        conn.close()
        exit(0)
    return ad_id

def __insert_into_price(conn: sql.Connection, cur: sql.Cursor, one_offer_data_for_price: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (price, date, ad_price)
        and write information to database
    """
    try:
        cur.execute('''INSERT INTO price(price, date, ad_price) VALUES (?, ?, ?);''', one_offer_data_for_price)
    except sql.OperationalError as err:
        print('Error:', err.__class__, err)
        cur.close()
        conn.close()
        exit(0)

def __insert_into_sq_per_m(conn: sql.Connection, cur: sql.Cursor, one_offer_data_for_sq_per_m: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (cost, ad_cost)
        and write information to database
    """
    try:
        cur.execute('''INSERT INTO cost_per_sq_m(cost, ad_cost) VALUES (?, ?);''', one_offer_data_for_sq_per_m)
    except sql.OperationalError as err:
        print('Error:', err.__class__, err)
        cur.close()
        conn.close()
        exit(0)

def __update_sq_per_m(conn: sql.Connection, cur: sql.Cursor, one_offer_data_for_sq_per_m: tuple) -> None:
    """ Function get sql.Connection, sql.Cursor and tuple (cost, ad_cost)
        and write information to database
    """
    try:
        cur.execute('''UPDATE cost_per_sq_m SET cost =? WHERE ad_cost = ?''', one_offer_data_for_sq_per_m)
    except sql.OperationalError as err:
        print('Error:', err.__class__, err)
        cur.close()
        conn.close()
        exit(0)