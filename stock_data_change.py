# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 13:45:31 2018

@author: Prerna
"""
import pandas as pd
from dateutil import parser
import numpy as np
# fill missing dates value in stock data and store it back in filesystem

def getISOFormatDate(date):
    date = parser.parse(date)
    return date.isoformat()

population = ['GOOGL', 'INTC', 'AAPL', 'CSCO', 'QCOM', 'NVDA', 'AMZN', 'MSFT', 'IBM']

for stock in population:
    stock_price_csv = pd.read_csv("price_data/"+ stock+"_2006-01-01_to_2017-11-01.csv")
    stock_price_csv['Ticker'] = stock
    stock_price_csv['Date'] = stock_price_csv['Date'].apply(getISOFormatDate)
    stock_price_csv['Date'] =  pd.to_datetime(stock_price_csv['Date'])
    stock_price_csv = stock_price_csv.replace('-', np.nan, regex=True)
    stock_price_csv = stock_price_csv.dropna()
    del stock_price_csv['Unnamed: 0']
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
    stock_price_csv.to_csv("new_price_data/"+ stock+"_2006-01-01_to_2017-11-01.csv")
    

def getUPDownClass(pctChange):
    if(pctChange < 0):
        return 0
    else:
        return 1




