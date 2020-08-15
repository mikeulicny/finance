import time
import csv
import requests
import psycopg2
import tdma

def add_equity_symbols(db_conn, symbol_list):
    cur = db_conn.cursor()

    client = tdma.Client()
    client.new_credentials()

    headers = {'Authorization': 'Bearer ' + client.access_token}

    for symbol in symbol_list:
        url = "https://api.tdameritrade.com/v1/instruments"
        payload = {
            'symbol': symbol,
            'projection': 'symbol-search'
        }
        r = requests.get(url, headers=headers, params=payload)
        data = r.json()


        try:
            if 'error' in data:
                raise tdma.ResponseError

            if data[symbol]['assetType'] == 'EQUITY':
                cur.execute("""INSERT INTO equities (symbol, description, exchange, cusip) VALUES (%s, %s, %s, %s);""",
                (data[symbol]['symbol'], data[symbol]['description'], data[symbol]['exchange'], data[symbol]['cusip']))
                print("{} data loaded into equities table".format(symbol))

            time.sleep(0.4)
            db_conn.commit()

        except tdma.ResponseError:
            print("Error in data retreived")
            print("symbol: {}".format(symbol))
            print("data: {}".format(data))

        except:
            print("Error in adding data to database")
            print("symbol: {}".format(symbol))
            print("data: {}".format(data))
            with open ('bad_symbols.txt', 'a') as f:
                f.write("%s\n" % symbol)

    cur.close()

def get_equity_symbols(db_conn):
    cur = db_conn.cursor()

    cur.execute("SELECT symbol FROM equities GROUP BY symbol")
    db_symbols = cur.fetchall()

    symbol_list = [it[0] for it in db_symbols]
    cur.close()

    return symbol_list

def get_etf_symbols(db_conn):
    cur = db_conn.cursor()

    cur.execute("SELECT symbol FROM etfs GROUP BY symbol")
    db_symbols = cur.fetchall()

    symbol_list = [it[0] for it in db_symbols]
    cur.close()
    return symbol_list