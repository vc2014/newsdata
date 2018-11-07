# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 17:55:56 2018

@author: visha
"""

import urllib.request
import pytz
import pandas_datareader.data as web
import datetime
from bs4 import BeautifulSoup
import csv


#### Section 1: Scrapes wikipedia page to get all tickers in the S&P 500

thisurl = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies" # the wikipedia url containing list of S&P 500 companies
# it helps to visit the webpage and take a look at the source to understand
#    how the html is parsed.

myPage = urllib.request.urlopen(thisurl) # opens this url

mySoup = BeautifulSoup(myPage, "html.parser") # parse html soup 

table = mySoup.find('table', {'class': 'wikitable sortable'}) # finds wiki sortable table in webpage html
ticker_security = dict()
sector_tickers = dict() # create a dictionary to store all tickers according to sector
for row in table.findAll('tr'): # find every row in the table
    col = row.findAll('td') # find every column in that row
    if len(col) > 0: # if there are columns in that row
        try:
            sector = str(col[2].string.strip()).lower().replace(' ', '_') # identify the sector in the row
            ticker = str(col[0].string.strip()) # identify the ticker in the row
            if sector not in sector_tickers: # if this sector is not a key in the dictionary
                sector_tickers[sector] = list() # add this as a key to the dictionary
            sector_tickers[sector].append(ticker) # add the ticker to right key in the dictionary
            security = str(col[1].string.strip()) # identify the security in the row
            ticker_security[ticker] = security
        except:
            print("Error")

#### Section 2: Queries Yahoo Finance for historical data on tickers

# Start and end dates for historical data
start = datetime.datetime(2010, 1, 1)  # start date
end = datetime.datetime(2018, 10, 31) # end date

myKeys = list(sector_tickers.keys()) # find all the sectors which are keys in the dictionary we created in Step 1

import os

for i in range(0,len(myKeys)): # for each key in the dictionary which represents a sector
    print(i)
    print(myKeys[i])
    myTickers = sector_tickers[myKeys[i]] # find the tickers in that list
    print(myTickers)
    if not os.path.exists(myKeys[i]):
        os.makedirs(myKeys[i])
    for j in range(0,len(myTickers)): # for each ticker
        try:
            myData = web.DataReader(myTickers[j], 'yahoo', start, end) # query the pandas datareader to pull data from Yahoo! finance
            fileName = "./" + myKeys[i] +'/'+ myTickers[j] + '.csv' # create a file
            myData.to_csv(fileName) # save data to the file
        except:
            print("exception")
            
            
import pandas as pd
from dateutil import parser
import numpy as np
# fill missing dates value in stock data and store it back in filesystem

def getISOFormatDate(date):
    date = parser.parse(date)
    return date.isoformat()

def getUPDownClass(pctChange):
    if(pctChange < 0):
        return 0
    else:
        return 1

for i in range(0,len(myKeys)): # for each key in the dictionary which represents a sector
    print(i)
    print(myKeys[i])
    myTickers = sector_tickers[myKeys[i]] # find the tickers in that list
    print(myTickers)
    
    for j in range(0,len(myTickers)): # for each ticker
        try:
            stock_price_csv = pd.read_csv(myKeys[i] + "/" + myTickers[j] + ".csv")
            stock_price_csv['Ticker'] = myTickers[j]
            stock_price_csv['Security'] = ticker_security[myTickers[j]]
            stock_price_csv['Sector'] = myKeys[i]
            stock_price_csv['Date'] = stock_price_csv['Date'].apply(getISOFormatDate)
            stock_price_csv['Date'] =  pd.to_datetime(stock_price_csv['Date'])
            stock_price_csv = stock_price_csv.dropna()
            stock_price_csv = stock_price_csv.sort_values(by='Date', ascending=[True])
            stock_price_csv.set_index('Date', inplace=True)
            stock_price_csv = stock_price_csv.resample('D').bfill().reset_index()
            close_price = stock_price_csv['Close']
            stock_volume = stock_price_csv['Volume']
            stock_price_csv['1_Day_Close_Change'] = close_price.astype(float).pct_change(1)
            stock_price_csv['1_Week_Close_Change'] = close_price.astype(float).pct_change(7)
            stock_price_csv['1_Month_Close_Change'] = close_price.astype(float).pct_change(30)
            stock_price_csv['1_Day_Volume_Change'] = stock_volume.astype(float).pct_change(1)
            stock_price_csv['1_Week_Volume_Change'] = stock_volume.astype(float).pct_change(7)
            stock_price_csv['1_Month_Volume_Change'] = stock_volume.astype(float).pct_change(30)
            stock_price_csv['Day_Class'] = stock_price_csv['1_Day_Close_Change'].apply(getUPDownClass)
            stock_price_csv['Month_Class'] = stock_price_csv['1_Month_Close_Change'].apply(getUPDownClass)
            stock_price_csv['Week_Class'] = stock_price_csv['1_Week_Close_Change'].apply(getUPDownClass)
            stock_price_csv = stock_price_csv.replace(np.nan, 0, regex=True)
            stock_price_csv.to_csv(myKeys[i] + "/" + myTickers[j] + "_mod.csv")
        except:
            print("exception")


import os

for i in range(0,len(myKeys)): # for each key in the dictionary which represents a sector
    print(i)
    print(myKeys[i])
    myTickers = sector_tickers[myKeys[i]] # find the tickers in that list
    print(myTickers)
    df = pd.DataFrame()
    for j in range(0,len(myTickers)): # for each ticker
        try:
            fileName = myKeys[i] +'/'+ myTickers[j] + '_mod.csv' # create a file
            stock_price_csv = pd.read_csv(fileName) # read data from the file
            df =  df.append(stock_price_csv, ignore_index=True)
        except:
            print("exception")
    df.to_csv(myKeys[i] + "/consolidated.csv")
                

df = pd.DataFrame()

for i in range(0,len(myKeys)): # for each key in the dictionary which represents a sector
    print(i)
    print(myKeys[i])
    try:
        fileName = myKeys[i] +'/'+ 'consolidated.csv' # create a file
        stock_price_csv = pd.read_csv(fileName) # read data from the file
        stock_price_csv = stock_price_csv[(stock_price_csv['1_Day_Close_Change'] <= -0.25) | ((stock_price_csv['1_Week_Close_Change'] <= -0.25) & (stock_price_csv['1_Day_Close_Change'] != 0)) ]
        df =  df.append(stock_price_csv, ignore_index=True)
    except:
        print("exception")

df = df[df["Open"] >= 1]        

df.groupby("Ticker").count()

df.to_csv("S&Pmidcap400_FinalResult.csv")
