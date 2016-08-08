# -*- coding: utf-8 -*-
"""
Created on Sun Aug  7 23:06:22 2016

@author: Xin
"""

import datetime
import requests
import sqlite3 as lite
import pandas as pd
import collections

APIKEY = 'b3d9e1886a328223d5afd670704cdf2e/'
start_date = datetime.datetime.now() - datetime.timedelta(days=30)

cities = { "Atlanta": '33.762909,-84.422675',
            "Austin": '30.303936,-97.754355',
            "Boston": '42.331960,-71.020173',
            "Chicago": '41.837551,-87.681844',
            "Cleveland": '41.478462,-81.679435'
        }

# create table in SQLite database
con = lite.connect('weather.db')
cur = con.cursor()

with con:
    cur.execute('CREATE TABLE temperature_max (date INT PRIMARY KEY, '+ ' FLOAT, '.join(cities.keys()) + ' FLOAT)')

# API call
# print ('https://api.forecast.io/forecast/' + APIKEY + cities["Boston"] + ',' + str(start_date))
for i in range(30):
    date = start_date + datetime.timedelta(days = i)
    cur.execute('INSERT INTO temperature_max (date) VALUES (?)', (date.strftime('%s'),))
    for k,v in cities.iteritems():
        r = requests.get('https://api.forecast.io/forecast/' + APIKEY + v + ',' + date.strftime('%s'))
        # 3 levels of data, get the daily maximum temperature "temperatureMax"
        daily = r.json()['daily']['data'][0]
        cur.execute('UPDATE temperature_max SET %s = %5.2f WHERE date = %s' % (k, daily['temperatureMax'], date.strftime('%s')))

# Profiling the temperature data
df = pd.read_sql_query('SELECT * FROM temperature_max ORDER BY date', con, index_col = 'date')

# range of temperatureMax
print("The range of maximum temperature in Boston from %s to %s is %5.2fF to %5.2fF." % ( 
        datetime.datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%d'),
        datetime.datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%d'),
        min(df['Boston']),
        max(df['Boston']),
    ))
# The range of maximum temperature in Boston from 2016-07-09 to 2016-08-07 is 62.17F to 95.15F

# mean and variance temperature for each city
for column in df:
    print("The mean of maximum temperature in " + column + " is " + "%.2f" % (df[column].mean()))
    print("The variance of maximum temperature in " + column + " is " + "%.2f" % (df[column].var()))

daily_change = collections.defaultdict(int)

for column in df:
    shift = df[column].diff()
    daily_change[column] = abs(shift[1:]).sum()

# Find the key with the greatest value
def maxtempchg(d):
    return max(d, key = lambda k: d[k])
    
# The city with greatest temperature variation
max_temp_change = maxtempchg(daily_change)  # Boston

# distribution of the absolute temperature difference
abs(shift[1:]).hist()
