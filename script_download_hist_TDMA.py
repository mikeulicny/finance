import requests
import json
import psycopg2
import time
import datetime
import tdma

from database_functions import get_equity_symbols, get_etf_symbols

def main():
    # connect to database
    conn = psycopg2.connect("dbname=financial_data user=mike")
    cur = conn.cursor()

    # connect to TD Ameritrade api
    client = tdma.Client()
    client.new_credentials()
    token_time = datetime.datetime.now()
    headers = {'Authorization': 'Bearer ' + client.access_token}
    payload = {
            'period': '1',
            'periodType': 'month',
            'frequency': '1',
            'frequencyType': 'daily'
        }

    while True:
        # get a set of previous symbols in daily database
        cur.execute("SELECT symbol FROM daily GROUP BY symbol")
        db_symbols = set(cur.fetchall())

        # get a list of current symbols
        symbols = get_equity_symbols(conn)
        symbols += get_etf_symbols(conn)

        for symbol in symbols:
            url = "https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(symbol)

            # TD Ameritrade access/authorization tokens expire every 30 minutes
            # if time from last access token is greater than 29 minutes request
            # a new token and set new authorization header
            delta = datetime.datetime.now() - token_time
            if delta.seconds >= 1740:
                client.new_credentials()
                headers = {'Authorization': 'Bearer ' + client.access_token}
                print(":: New TD Ameritrade access token requested")
                token_time = datetime.datetime.now()
            
            # If the symbol exists in daily database get a set of timestamps for the symbol
            if (symbol,) in db_symbols:
                cur.execute("SELECT datetime FROM daily WHERE symbol = %s;",(symbol,))
                symbol_timestamps = set(cur.fetchall())

            # request and parse data
            r = requests.get(url, headers=headers, params=payload)
            data = r.json()

            try:
                if 'error' in data:
                    raise tdma.ResponseError(data)

                if data['empty'] == True:
                    raise tdma.NoDataError()
                
                lines = 0
                candles = data['candles']
                for day in candles:

                    # convert ms since epoch into a formatted timestamp for postgresql
                    # symbol and datetime columns are unique constraints in the database
                    # default time to 00:00:00 for all daily data to prevent duplication
                    dt = datetime.datetime.fromtimestamp(day['datetime'] / 1000.0).strftime('%Y-%m-%d')
                    
                    # if the timestamp for the symbol exists skip to the next day's data
                    if (datetime.datetime.strptime(dt, '%Y-%m-%d'),) in symbol_timestamps:
                        continue

                    # replace any "NaN" strings with python None values. This will input NULL into
                    # the postgresql database location instead of throwing an error
                    if day['open'] == "NaN":
                        day['open'] = None

                    if day['high'] == "NaN":
                        day['high'] = None

                    if day['low'] == "NaN":
                        day['low'] = None

                    if day['close'] == "NaN":
                        day['close'] = None

                    if day['volume'] == "NaN":
                        day['volume'] = None

                    # insert data into database
                    cur.execute("""INSERT INTO daily (symbol, datetime, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);""",
                        (symbol, dt, day['open'], day['high'], day['low'], day['close'], day['volume']))
                    
                    lines += 1
                    
                if lines != 0:
                    # only commit changes on all of a symbols data if any was inserted
                    conn.commit()
                print("> [{}] {} rows added to daily database".format(symbol, lines))

            except tdma.ResponseError as re:
                print("Error in TD Ameritrade request for symbol: {}".format(symbol))
                print("Data returned: {}".format(re))

            except tdma.NoDataError:
                print("Error: [{}] has no timeseries sales data".format(symbol))

            # create delay for TD Ameritrade requests. TD Ameritrade has a max requests/second. If this
            # limit is hit the users app will be locked out of sending requests for a few minutes
            time.sleep(0.5)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()