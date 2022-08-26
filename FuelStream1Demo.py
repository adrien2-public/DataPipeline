#!/usr/bin/python

# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import random
import time
import csv
from datetime import date
from datetime import datetime 
from datetime import timedelta
from urllib.request import urlopen
import json



# create fake addresses
with urlopen("https://raw.githubusercontent.com/EthanRBrown/rrad/master/addresses-us-all.json") as response:
    source = response.read()

data = json.loads(source.decode("utf-8"))
addresses =[]
list = data['addresses'][:3219]
for c in list:
    try:
        addresses.append((c["address1"]+' '+c["city"]+ ' ' + c['state']))
    except KeyError:
        addresses.append((c["address1"]+' '+'nowhereland'+ ' ' + c['state']))


# create fake timestamps
timestamp = pd.date_range(date.today(), periods=288, freq='1min') # create date range for 1 day in 1 minute intervals , this gives approx 288 periods
# makes no sense to run data for whole day as this would exceed my free tier limit for lambda, so we do it in batches for 3 sets of requests
first =timestamp[0:5] # first set of data, within first 5 miutes of date
second =timestamp[5:9] # 2nd set of data, between 5 and 10 minutes from start of day  
third =timestamp[9:13] # 3rd set of data, between 10 and 15 minutes from start of day  

#below defines a small subset of alarms and params that are generated by equipment on site, repeats are to reduce probability that everything gets flagged in ur lambda function
atg_alarms = ['WATER DETECTED','404 ERROR PROBE','NONE','TESTING']
line_alarms = ['AIR PRESSURE ','SLOW FLOW','NONE','TESTING','NONE'] 
alarms_on_off = ['YES','NO']
pump_state = ['TESTING', 'OUT OF SERVICE','NONE']

# create fake site list
Sites = range(1,3200,1) # each site (3200 of them) has 3 products with 3 tanks and lines
Grades = ['REG','PRE','DSL']

#create function to write to file
def MakeStreamFuel(k):
        AlarmStream = time.strftime("/var/log/fuelstream5min/AlarmStream-%Y%m%d-%H%M%S.log") # specify location where log files will be saved
        with open(AlarmStream, 'w') as file:
 

                
                headers = ['SiteID','address','atg_id', 'atg','line', 'default_alarm','dispenser', 'timestamp']
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()	


                for Site, c in zip(Sites,addresses):
                        for Grade in Grades:
                                atg_id = (Grade + str(Site))
                                atg = random.choices(atg_alarms, weights= [3,6,50,5])
                                line = random.choices(line_alarms, weights= [3,8,10,35])
                                default_alarm = random.choices(alarms_on_off, weights= [2,50] )
                                dispenser = random.choices(pump_state, weights= [10,5,70])
                                timestmp = random.choice(k)
                                writer.writerow({'SiteID': Site, 'address': c ,'atg_id': atg_id ,'atg': atg, 'line':line ,'default_alarm': default_alarm, 'dispenser':  dispenser, 'timestamp': timestmp })
        

        
        

MakeStreamFuel(first)
time.sleep(300)
print("5 minuts passed")
MakeStreamFuel(second)
time.sleep(300)
print("10 minuts passed")
MakeStreamFuel(third)
time.sleep(300)
print("15 minuts passed")
MakeStreamFuel(fourth)
  
  
      
    

