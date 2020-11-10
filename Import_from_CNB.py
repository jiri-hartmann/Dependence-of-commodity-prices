#!/usr/bin/python
""" Download exchange rates inforation from CNB server (www.cnb.cz) and upload to local database named ades. 
    Program find day of last entry to db., download  and import next days up to today. 
"""

import datetime as dt
import pandas as pd
import sys
from make_mysql_connection import make_mysql_connection


CNB_ER_COLUMNS = {"Datum":"Date", "1 AUD":"AUD", "1 BGN":"BGN", "1 BRL":"BRL", "1 CAD":"CAD", "1 CHF":"CHF", 
                  "1 CNY":"CNY", "1 DKK":"DKK", "1 EUR":"EUR", "1 GBP":"GBP", "1 HKD":"HKD", 
                  "1 HRK":"HRK", "100 HUF":"HUF100", "1000 IDR":"IDR1000", "1 ILS":"ILS", 
                  "100 INR":"INR100", "100 ISK":"ISK100", "100 JPY":"JPY100", "100 KRW":"KRW100", 
                  "1 MXN":"MXN", "1 MYR":"MYR", "1 NOK":"NOK", "1 NZD":"NZD", "100 PHP":"PHP100", 
                  "1 PLN":"PLN", "1 RON":"RON", "100 RUB":"RUB100", "1 SEK":"SEK", "1 SGD":"SGD", 
                  "100 THB":"THB100", "1 TRY":"TRY", "1 USD":"USD", "1 XDR":"XDR", "1 ZAR":"ZAR"}  
                   # definition of pse_ak table

try:
    alchemy_conn = make_mysql_connection()   
    first_day_for_import = alchemy_conn.execute("select max(`date`) from cnb_er").fetchone()[0] + dt.timedelta(days=1)
except:
    print("Connection to database failed")
    sys.exit(0)    

try:
    year = first_day_for_import.strftime('%Y')
    connect_string = (f"https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/rok.txt?rok={year}")
    cnb_er = pd.read_table(connect_string, sep="|", decimal=",")
except:
    print("CNB: connection to server failed.")
    sys.exit(0)

cnb_er = cnb_er.rename(CNB_ER_COLUMNS, axis=1, errors="raise")  # name column is the same as in database
                                                # used for check if column are not changed between years
cnb_er['Date'] = pd.to_datetime(cnb_er.Date, format='%d.%m.%Y') # convert to yyyy-mm-dd 

cnb_er.set_index("Date", inplace = True)                        
cnb_er = cnb_er.loc[first_day_for_import : dt.date.today()]     # select for import only new days

cnb_er.to_sql("cnb_er", alchemy_conn, if_exists= "append") 
if not cnb_er.empty:
    pd.set_option('display.max_rows', 50)
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    print(cnb_er[["EUR", "USD"]])
