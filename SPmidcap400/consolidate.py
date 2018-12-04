# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 14:01:42 2018

@author: visha
"""
import pandas as pd
import os
import datetime
       
myKeys = list(sector_tickers.keys()) # find all the sectors which are keys in the dictionary we created in Step 1

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
        stock_price_csv = stock_price_csv[(stock_price_csv['1_Day_Close_Change'] <= -0.3) | (stock_price_csv['1_Week_Close_Change'] <= -0.3) ]
        df =  df.append(stock_price_csv, ignore_index=True)
    except:
        print("exception")

df = df[df["Open"] >= 1]        
df.to_csv("DayWeekChange.csv")


df['Date'] =  pd.to_datetime(df['Date'])

df = df[(df['Date'] >= datetime.datetime(2010,1,1))]

df.groupby("Ticker").count()

df.to_csv("S&Pmidcap400_FinalResult.csv")