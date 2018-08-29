# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 00:04:38 2018

@author: Prerna
"""

import pandas as pd

data = pd.read_csv('merged_data.csv')
data = data.dropna()

rows = []
for index, row in data.iterrows():
    columns = row[0][1:len(row[0])-1].split(",")
    rows.append((columns[0], columns[1], columns[2], columns[3], columns[4], columns[5]))
    
columnName = ["Ticker", "Organization", "Time", "Headline", "Body", "Type"]
data = pd.DataFrame(rows, columns = columnName)

data.to_csv('news_data.csv')

data




data.Headline_Ext = data.Headline + "." + data.Body

Text = ''.join(text for text in data.Headline_Ext)

import re
Text = re.sub('[^A-Za-z0-9]+', ' ', Text)

Text = re.sub('[0-9]+', ' #NUMBER ', Text)
              
from nltk.tokenize import sent_tokenize
sent_tokenize_list = sent_tokenize(Text)

from nltk.tokenize import word_tokenize

sentence_token = []
for sentence in sent_tokenize_list:
    sentence_token.append([word for word in word_tokenize(sentence) if len(word) >= 2])

from gensim.models import Word2Vec    
model = Word2Vec(sentence_token, size=100, window=5, min_count=1, workers=4)    


wv = model.wv

wv.get_vector('Facebook')
word2index = {token:token_index for token_index, token in enumerate(wv.index2word)}

word2index['Facebook']


