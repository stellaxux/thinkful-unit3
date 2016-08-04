# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 22:13:59 2016

@author: Xin
"""

import requests
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import sqlite3 as lite
import time
from dateutil.parser import parse
import collections

# access the NYC citibike URL
r = requests.get('http://www.citibikenyc.com/stations/json')

# get a basic view of the text type
# r.text

# format data into JSON
# r.json()

# get a list of keys
r.json().keys()
# [u'executionTime', u'stationBeanList']

# get the list of keys for each station listing
key_list = []
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)
print key_list

# convert the data into a pandas DataFrame
df = json_normalize(r.json()['stationBeanList'])

# checking the ranges of values
df['availableBikes'].hist()
plt.show()

df['totalDocks'].hist()
plt.show()

df['availableDocks'].hist()
plt.show()

# numbers of test station
sum(df['testStation'])  # 0, no test station

# numbers of station in service/not in service
sum(df['statusValue'] == 'In Service') # 522
sum(df['statusValue'] == 'Not In Service') # 142

# mean number of availablebikes
df['availableBikes'].mean()  # 9.426204819277109
df['availableBikes'].median()  # 5.0
df[df['statusValue'] == 'In Service'].availableBikes.mean() # 11.99
df[df['statusValue'] == 'In Service'].availableBikes.median()  # 10

con = lite.connect('citi_bike.db')
cur = con.cursor()

with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, \
    totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude \
    NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, \
    stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
    
#a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, \
    stAddress2, longitude, postalCode, testStation, stAddress1, stationName,\
    landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],\
        station['altitude'],station['stAddress2'],station['longitude'],\
        station['postalCode'],station['testStation'],station['stAddress1'],\
        station['stationName'],station['landMark'],station['latitude'],\
        station['location']))
        
# extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist()

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]
        
#create the table
#in this case, we're concatenating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes (execution_time INT, " + \
    ", ".join(station_ids) + ");")
    
#take the string and parse it into a Python datetime object
exec_time = parse(r.json()['executionTime'])

# inserting exec_time into the database
with con:
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', \
    (exec_time.strftime('%s'),))

# use defaultdict to store available bikes by station
id_bikes = collections.defaultdict(int)

#loop through stations in the list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']
    
# update values in the database
with con:
    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) \
        + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
