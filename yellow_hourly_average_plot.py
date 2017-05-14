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
      row = line.split(',')
      pick = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') #2015-01-08 22:44:09
      drop = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
      duration = (drop - pick).total_seconds()/60 # for each 10 minutes
      #if hour >= 7 and hour <= 9:
      yield (float(row[6]), float(row[5]), int(row[0]), float(row[10]), float(row[9]), pick.hour,int(duration)/10)
         
def get_miles(part):
   start = (part[0], part[1])
   end = (part[3], part[4])
   m = vincenty(start,end).miles
   return (part[6], m)
   

# create rdd, read from yellow files
#convert to data frame
def read_yellow_to_dataframe():                      
   # create fields to give csv structure
   field_name = ['start_latitude','start_longitude', 'vendor_id', 'end_latitude', 'end_longitude','starttime','duration']   
   field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), IntegerType(),IntegerType()]
   # create schema
   field=[]
   for i in range(0,7):
      field.append(StructField(field_name[i], field_type[i]))
   schema = StructType(field)  
   cur = '/user/gdicarl000/projectdata/cardata.csv'
   c2 = sc.textFile(cur).mapPartitionsWithIndex(parseYELLOWCSV)
   df = sqlContext.createDataFrame(c2,schema)
   return df

def save_dataframe_to_plot(df):
   # for miles
   avgRDD = yellow.rdd.map(get_miles) # duration per mile
   avgRDD= avgRDD.mapValues(lambda x: (x,1))
   avgRDD = avgRDD.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
   avgRDD= avgRDD.mapValues(lambda y : 1.0 * y[0] / y[1])
   #avgRDD.show(2)
   schema2 = StructType([StructField("Hour", IntegerType()), StructField("Miles", FloatType())])
   mdf = sqlContext.createDataFrame(avgRDD,schema2)
   #mdf.show(10)
   # convert to panda df
   mpdDF = mdf.toPandas()
   return mpDF

# get combined dataframe of all
yellow =read_yellow_to_dataframe()
yellow.show(1)
yellowMpdDF = save_dataframe_to_plot(yellow)

YellowsDF=yellowMpdDF.sort_values('Hour',  ascending=False)         
YellowsDF.plot(x='Hour', y='Miles',linestyle='--', marker='X', color='b', kind='line',grid=True)
plt.savefig("yellow_by_hour.png")     

