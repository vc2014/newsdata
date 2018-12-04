# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 13:41:28 2018

@author: visha
"""

import pandas as pd
from dateutil import parser
import numpy as np
# fill missing dates value in stock data and store it back in filesystem

def getISOFormatDate(date):
    date = parser.parse(date)
    return date.isoformat()

for i in range(0,len(myKeys)): # for each key in the dictionary which represents a sector
    print(i)
    print(myKeys[i])
    myTickers = sector_tickers[myKeys[i]] # find the tickers in that list
    print(myTickers)
    
    for j in range(0,len(myTickers)): # for each ticker
        try:
            stock_price_csv = pd.read_csv(myKeys[i] + "/" + myTickers[j] + ".csv")
            stock_price_csv['Ticker'] = myTickers[j]
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



def getUPDownClass(pctChange):
    if(pctChange < 0):
        return 0
    else:
        return 1
