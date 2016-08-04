# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 22:13:59 2016

@author: Xin
"""

import requests
import sqlite3 as lite
import time
from dateutil.parser import parse

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
