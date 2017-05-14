#run in pyspark not spark-submit, can do that too but need to add some lines

from pyspark import SparkContext
from pyspark.sql import SQLContext
from datetime import datetime
from datetime import timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import csv
from geopy.distance import vincenty
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
plt.switch_backend('agg') # to enable plot in backend


'''
0 VendorID,
1 tpep_pickup_datetime
2 tpep_dropoff_datetime
3 passenger_count
4 trip_distance
5 pickup_longitude
6 pickup_latitude
7 RateCodeID
8 store_and_fwd_flag
9 dropoff_longitude
10 dropoff_latitude
11 payment_type
12 fare_amount
13 extra
14 mta_tax
15 tip_amount
16 tolls_amount
17 total_amount
'''

#parse yellow csv
def parseYELLOWCSV(idx, part):
   if(idx==0):
      part.next()
   for line in part:
      try: 
         row = line.split(',')
         pick = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') #2015-01-08 22:44:09
         drop = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
         duration = (drop - pick).total_seconds()/60 # for each 10 minutes
         #if hour >= 7 and hour <= 9:
         yield (float(row[6]), float(row[5]), int(row[0]), float(row[10]), float(row[9]), pick.hour,int(duration)/10)
      except:
         continue
         
def get_yellow_schema():
   field=[]
   field_name = ['start_latitude','start_longitude', 'vendor_id', 'end_latitude', 'end_longitude','starttime','duration']   
   field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), IntegerType(),IntegerType()]
   for i in range(0,7):
      field.append(StructField(field_name[i], field_type[i]))
   schema = StructType(field) 
   return schema
     
# create rdd, read from yellow files
#convert to data frame
def read_yellow_to_dataframe():                      
   # create fields to give csv structure
   # create schema
   schema = get_yellow_schema()
   cur = '/user/gdicarl000/projectdata/cardata.csv'
   c2 = sc.textFile(cur).mapPartitionsWithIndex(parseYELLOWCSV)
   df = sqlContext.createDataFrame(c2,schema)
   return df

# def save_dataframe_to_plot(df) defined in citibike py
# def get_miles(part) defined in citibike py
# def get_plot_df(df, hour)
# save_plot_by_hour 

# get combined dataframe of all
yellow =read_yellow_to_dataframe()
yellow.show(1)
save_plot_by_hour(yellow)
#yellowMpdDF = save_dataframe_to_plot(yellow)

#YellowsDF=yellowMpdDF.sort_values('Hour',  ascending=False)         
#YellowsDF.plot(x='Minutes', y='Miles',linestyle='--', marker='X', color='b', kind='line',grid=True)
#plt.savefig("yellow_by_hour.png")     

def create_items():
   yellow = read_yellow_to_dataframe()
   citi =get_one_citi()
   save_plot_by_hour(citi)
   save_plot_by_hour(yellow)

