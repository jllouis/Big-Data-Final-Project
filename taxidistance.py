import plotly.plotly as py
from plotly.graph_objs import *
import pandas as pd
import requests
from math import cos, asin, sqrt
import re
#don't need all of these imports, I have them a lot of them for when I start graphing 

taxi_data = "C:/Users/_______/Downloads/cleantaxi_morning.txt"
taxi = open(taxi_data,'r')
taxi_list = taxi.read().strip().split(',') #open taxi data and read it into a list that is split up by commas (delimmiting character)
taxi_list = taxi_list[6:len(taxi_list)]

distance = []
#need to get every nth item in the list, split up into their proper lists
lat1_entry = taxi_list[1::6]
lat2_entry = taxi_list[3::6]
lon1_entry = taxi_list[0::6]
lon2_entry = taxi_list[2::6]

#2 data points have random quotation marks
pattern = re.compile('"')
#regular expression to ensure that there are no quotation marks(bad data)
for i in range(0,len(lat1_entry)):
    match = re.search(pattern, lat1_entry[i])
    if match:
        continue #if quotation mark skip calculation and move to next iteration
    match2 = re.search(pattern, lat2_entry[i])
    if match2:
        continue
    match3 = re.search(pattern, lon1_entry[i])
    if match3:
        continue
    match4 = re.search(pattern, lon2_entry[i])
    if match4: 
        continue
    else: 
        try: #calculate distance in km
            lat2 = float(lat2_entry[i]) 
            lat1 = float(lat1_entry[i])
            lon1 = float(lon1_entry[i])
            lon2 = float(lon2_entry[i])
            p = 0.017453292519943295     #Pi/180
            a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
            dist = 12742 * asin(sqrt(a))
            distance.append(dist)
        except:
            continue
            
