# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 22:13:59 2016

@author: Xin
"""

import requests
import sqlite3 as lite
import time
import collections
import pandas as pd
from dateutil.parser import parse
import matplotlib.pyplot as plt
import datetime

con = lite.connect('citi_bike.db')
cur = con.cursor()

for i in range(60):
# access the NYC citibike URL
    r = requests.get('http://www.citibikenyc.com/stations/json')
    
#take the string and parse it into a Python datetime object
    exec_time = parse(r.json()['executionTime']).strftime("%s")

# inserting exec_time into the database
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', \
    (exec_time,))

#loop through stations in the list and update values in the database
    for station in r.json()['stationBeanList']:
        cur.execute("UPDATE available_bikes SET _%d = %d WHERE execution_time = %s" \
        % (station['id'], station['availableBikes'], exec_time))
    con.commit()
    
    time.sleep(60)

#close the database connection when done
con.close()

# collect and store the change for each station every minute
con = lite.connect('citi_bike.db')
cur = con.cursor()

df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time",con,index_col='execution_time')
hour_change = collections.defaultdict(int)

for col in df.columns:
    station_id = col[1:]
    station_change = df[col].diff()
    hour_change[int(station_id)] = abs(station_change[1:]).sum()

# Find the key with the greatest value
def maxstation(d):
    return max(d, key = lambda k: d[k])
    
# assign the max key to max_station
max_station = maxstation(hour_change)

#query sqlite for reference information
cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()

print("The most active station is station id %s at %s latitude: %s longitude: %s " % data)
print("With %d bicycles coming and going in the hour between %s and %s" % (
    hour_change[max_station],
    datetime.datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%d %H:%M:%S'),
    datetime.datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%d %H:%M:%S'),
))

plt.bar(hour_change.keys(), hour_change.values())
plt.show()