# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 11:54:41 2018

@author: Prerna
"""

import pandas as pd
from dateutil import parser

population = ['GOOGL', 'INTC', 'AAPL', 'CSCO', 'QCOM', 'NVDA', 'AMZN', 'MSFT', 'IBM']
events = []
Days = []
event_2_embedding = []
tickers = []
headlines = []
for stock in population:
    for event in stock_to_events[stock]:
        day = parser.parse(event[0])
        Days.append(day.isoformat())
        ticker, time, headline = index2info[event[1]]
        event_2_embedding.append(embedding_lst[event[1]])
        tickers.append(ticker)
        headlines.append(headline)
        

FinalDF = pd.DataFrame(Days, columns=["Date"])
FinalDF["Ticker"] = tickers
FinalDF["Headline"] = headlines
FinalDF["Event_Embedding"] = event_2_embedding
 


StockPriceDF = pd.DataFrame()

def getISOFormatDate(date):
    date = parser.parse(date)
    return date.isoformat()


for stock in population:
    stock_price_csv = pd.read_csv("new_price_data/"+ stock+"_2006-01-01_to_2017-11-01.csv")
    stock_price_csv['Ticker'] = stock
    StockPriceDF = StockPriceDF.append(stock_price_csv)


StockPriceDF['Date'] = StockPriceDF['Date'].apply(getISOFormatDate)
del StockPriceDF['Unnamed: 0']

MergeDF = pd.merge(FinalDF, StockPriceDF, how='left', on=['Ticker', 'Date'],
         left_index=False, right_index=False, sort=True,
         suffixes=('_x', '_y'), copy=True, indicator=False,
         validate=None)


