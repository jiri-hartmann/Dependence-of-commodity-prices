#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Download daily stock inforation from PSE ftp server (ftp.pse.cz) and upload to local database named ades. 
    Program find day of last entry to db., download  and import next days up to today. """
__author__      = "Jiří Hartmann"
__email__ = "jiri.hartmann@gmail.com"

from sys import exc_info
import datetime as dt
from ftplib import FTP
from zipfile import ZipFile 
import pandas as pd
from make_mysql_connection import make_mysql_connection


PSE_AK_COLUMN = ["isin", "name", "bic", "date", "price_closing", "price_change", "price_previous",
             "price_yearly_min", "price_yearly_max", "count_of_securities", "volume_of_deals", 
             "last_deal_date", "business_group", "trading_mode", "market_code", "price_daily_min", 
             "price_daily_max", "price_opening", "lot_size"]  # definition of pse_ak table


try:
    alchemy_conn = make_mysql_connection()
    date_from_date = alchemy_conn.execute("select max(`date`) from pse_ak").fetchone()[0] + dt.timedelta(1)
    # imported max(date) from db, format YYYY-mm-dd
    # date_from = "2013-01-01"  optional metod when db is new and empty
    # date_from_date = dt.datetime.strptime(date_from, '%Y-%m-%d').date()
except:
    print("Connection to database failed")
    quit()

try:    
    ftp = FTP("ftp.pse.cz")                    # connect to host, default port
    ftp.login()                                # user anonymous, passwd anonymous@
    ftp.cwd("Results.ak")                      # change into directory with free data
except:
    print("PSE: connection to ftp server failed.")
    quit()

delta = dt.date.today() - date_from_date              # as timedelta, today is accesible source file from yesterday
for i in range(delta.days):
    day = date_from_date + dt.timedelta(days=i)
    date_for_file = str(day).replace("-", "")[2:]     # "201104" for 2020-11-04
    import_zip_file = f"pl{date_for_file}.zip"
    try:
        with open("import/pl.zip", "wb") as fp:    # open local file "pl.zip" for writing
            ftp.retrbinary(f"RETR {import_zip_file}", fp.write)
            print(f"PSE: done downloading file {import_zip_file}.", end =" ") 
    except:
        print(f"PSE: File {import_zip_file} is not found.") 
        continue

    with ZipFile("import/pl.zip", "r") as zip: 
        zip.extract(f"AK{date_for_file}.csv", path = "import") 
        print("PSE: done unzipping file pl.zip.") 

    pse_ak = pd.read_csv(f"import/AK{date_for_file}.csv", encoding = "cp1250", names=PSE_AK_COLUMN, header=None) 
    pse_ak.to_sql("pse_ak", alchemy_conn, if_exists= "append", index = False) 
          
ftp.quit()