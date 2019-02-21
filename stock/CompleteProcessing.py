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



# Start and end dates for historical data
start = datetime.datetime(2010, 1, 1)  # start date
end = datetime.datetime(2019, 2, 10) # end date


import os
myTickers = ['0700.HK', 'PURE.L','BABA','FMG.AX','BYAN.JK','2318.HK','JD','9984.T','0384.HK','0678.HK','ADRO.JK','3988.HK','601117.SS','0005.HK','MEDC.JK','5099.KL','E5H.SI','MEGA.JK','AUTN.SW','ILD.PA','IMPN.SW','MARK.QA','MTNOY','QIBK.QA','QNBK.QA']
for j in range(0,len(myTickers)): # for each ticker
    try:
        myData = web.DataReader(myTickers[j], 'yahoo', start, end) # query the pandas datareader to pull data from Yahoo! finance
        fileName = "./" + myTickers[j] + '.csv' # create a file
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

    
for j in range(0,len(myTickers)): # for each ticker
    try:
        stock_price_csv = pd.read_csv("./" + myTickers[j] + ".csv")
        stock_price_csv['Ticker'] = myTickers[j]
        stock_price_csv['Organization'] = ""
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
        stock_price_csv.to_csv("./" + myTickers[j] + "_mod.csv")
    except:
        print("exception")


import os

df = pd.DataFrame()
for j in range(0,len(myTickers)): # for each ticker
    try:
        fileName = './'+ myTickers[j] + '_mod.csv' # create a file
        stock_price_csv = pd.read_csv(fileName) # read data from the file
        df =  df.append(stock_price_csv, ignore_index=True)
    except:
        print("exception")
df.to_csv("/consolidated.csv")
                

df = pd.DataFrame()


try:
    fileName =  '/consolidated.csv' # create a file
    stock_price_csv = pd.read_csv(fileName) # read data from the file
    stock_price_csv = stock_price_csv[(stock_price_csv['1_Day_Close_Change'] <= -0.25) | ((stock_price_csv['1_Week_Close_Change'] <= -0.25) & (stock_price_csv['1_Day_Close_Change'] != 0)) ]
    df =  df.append(stock_price_csv, ignore_index=True)
except:
    print("exception")

df = df[df["Open"] >= 1]        

df.groupby("Ticker").count()

df.to_csv("FinalResult.csv")