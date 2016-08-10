# -*- coding: utf-8 -*-
"""
Created on Tue Aug  9 11:50:34 2016

@author: Xin
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import sqlite3 as lite
import numpy as np

url = "http://web.archive.org/web/20110514112442/http://unstats.un.org/unsd/demographic/products/socind/education.htm"
r = requests.get(url)

soup = BeautifulSoup(r.content)

#for row in soup('table'):
#    print(row)

# locate the table
tb = soup('table')[6]('table')[1]

# find all the tags
#for tag in tb.find_all(True):
#    print(tag.name)

# return all the strings in the table
content = []
for string in tb.tr.stripped_strings:
    if string in ['a','b','c','d','e','f','g','h']:
        continue
    content.append(string)
content = content[9:]
header = ['Country or area', 'Year', 'Total', 'Men', 'Women']

# reshape and put them into pandas dataframe
content_reshape = [content[i:i+5] for i in range(0, len(content), 5)]
df = pd.DataFrame(content_reshape, columns = header)
df[['Total', 'Men', 'Women']] = df[['Total', 'Men', 'Women']].astype(int) 

# plot distribution
df['Men'].hist()
df['Women'].hist()
df['Total'].hist()

# median and mean age for each gender
df['Men'].mean()
df['Women'].mean()
df['Total'].mean()

df['Men'].median()
df['Women'].median()
df['Total'].median()


# create gdp database
con = lite.connect('gdp.db')
cur = con.cursor()
with con:
    cur.execute("DROP TABLE IF EXISTS gdp")
    cur.execute('CREATE TABLE gdp (country_name TEXT, _1999 NUMERIC, _2000 NUMERIC, _2001 NUMERIC,\
                 _2002 NUMERIC, _2003 NUMERIC, _2004 NUMERIC, _2005 NUMERIC, _2006 NUMERIC, _2007 NUMERIC, _2008 NUMERIC, _2009 NUMERIC, _2010 NUMERIC)')

## Compare GDP to education attainment
with open('world_bank_data/GDP.csv', 'rU') as inputFile:
    next(inputFile) # skip the first two lines
    next(inputFile)
    next(inputFile)
    next(inputFile)
    header = next(inputFile)
    inputReader = csv.reader(inputFile)
    for line in inputReader:
        with con:
            cur.execute('INSERT INTO gdp (country_name, _1999, _2000, _2001,\
            _2002, _2003, _2004, _2005, _2006, _2007, _2008, _2009, _2010) VALUES ("'+line[0]+'", "'+'","'.join(line[43:55]) + '");')

table_gdp = pd.read_sql_query('SELECT * FROM gdp', con)

# find common countries
country_gdp = table_gdp['country_name'].tolist()
country_un = df['Country or area'].tolist()
country_common = list(set(country_gdp) & set(country_un))

gdp = []
total = []
men = []
women = []
for country in country_common:
    df1 = df[df['Country or area']==country]
    df2 = table_gdp[table_gdp['country_name']==country]
    if (df2['_'+ df1['Year']].iloc[0].iloc[0] != ''):
        total.append(df1['Total'].iloc[0])
        men.append(df1['Men'].iloc[0])
        women.append(df1['Women'].iloc[0])
        gdp.append(np.log(df2['_'+ df1['Year']].iloc[0].iloc[0]))
gdp_schoollife = pd.DataFrame({'Total': total, 'Men': men, 'Women': women, 'GDP': gdp})

print gdp_schoollife.corr()
