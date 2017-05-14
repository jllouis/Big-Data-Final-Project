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
from py4j.protocol import Py4JJavaError

'''0) tripduration,   => 634
   1) starttime,      => 2013-07-01 00:00:00  datetime.strptime('2013-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')
   2) stoptime,       => 2013-07-01 00:10:34
   3) start station id, => 164
   4) start station name, => E 47 St & 2 Ave
   5) start station latitude, => 40.75323098
   6) start station longitude, => -73.97032517
   7) end station id,   => 504
   8) end station name,  => 1 Ave & E 15 St
   9) end station latitude,=> 40.73221853
   10) end station longitude, => -73.98165557
   11) bikeid,        => 16950
   12) usertype,      => Customer
   13) birth year,    => \N
   14) gender,        => 0

'''
def get_citi_schema():
      field_name = ['start_latitude','start_longitude', 'vendor_id', 'end_latitude', 'end_longitude','starttime','duration']   
      field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), IntegerType(),IntegerType()]
      # create schema
      field=[]
      for i in range(0,7):
            field.append(StructField(field_name[i], field_type[i]))
      schema = StructType(field)# get next month
      return schema
   
def next_month(start):
   month = start.month
   while month == start.month:
      start+=timedelta(days = 1)
   return start

#parse citi csv
def parseCITIBIKECSV(idx, part):
   if(idx==0):
      part.next()
   for line in part:
      row = line.split(',')
      try:
         duration_bucket = int(float(row[0][1:-1])/10)*10
         date = row[1][1:-1].split(' ')
         hour = date[1].split(':')[0]
         #if hour >= 7 and hour <= 9:
         yield (float(row[5][1:-1]),float(row[6][1:-1]),int(row[7][1:-1]),float(row[9][1:-1]),float(row[10][1:-1]),int(hour), duration_bucket)
      except:
         continue
         
def get_miles(part):
      start = (part[0], part[1])
      end = (part[3], part[4])
      m = vincenty(start,end).miles
      return (part[6], m)
   
# create rdd, read from citibike files
# map rdd to a schema we want
# define that schema
# convert rdd to rdd using that scmema
def read_citibike_to_dataframe():                      
   end = datetime.strptime('201601', '%Y%m')
   start = datetime.strptime('201501', '%Y%m')
   # create fields to give csv structure
   # create schema
   schema=get_citi_schema()
   print"Start reading"
   while start < end:
      cur = '/user/gdicarl000/projectdata/citibike/'+start.strftime('%Y%m')+'-citibike-tripdata.csv'
      c = sc.textFile(cur).cache()
      c2 = c.mapPartitionsWithIndex(parseCITIBIKECSV)
      df = sqlContext.createDataFrame(c2,schema)
      start= next_month(start)
      #df.show(10)
      print cur
      #print c2.count()
      yield df
        
def get_single():
      cur = '/user/gdicarl000/projectdata/citibike/201307-citibike-tripdata.csv'
      r = sc.textFile(cur).cache()
      c_single = r.mapPartitionsWithIndex(parseCITIBIKECSV)
      schema = get_citi_schema()
      df = sqlContext.createDataFrame(c_single,schema)
      return df
      
def get_plot_df(df, hour):
   # for miles
   avgRDD = df.rdd.filter(lambda x: x[5] == hour)
   avgRDD= avgRDD.map(get_miles).filter(lambda x: x[1] >0).mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda y : 1.0 * y[0] / y[1])
   #avgRDD.show(2)
   schema2 = StructType([StructField("Minutes", IntegerType()), StructField("Miles", FloatType())])
   mdf = sqlContext.createDataFrame(avgRDD,schema2)
   #mdf.show(10)
   return mdf

def save_plot_by_hour(df):
   for hr in range(0,24):
      mdf = get_plot_df(df, hr)
      # convert to panda df
      CitiAvgDF = mdf.toPandas()
      quantile=CitiAvgDF['Minutes'].quantile(.95)
      for index, row in CitiAvgDF.iterrows():
         if (row["Minutes"] >= quantile):
            CitiAvgDF.drop(index, inplace=True)
      CitiAvgDF=CitiAvgDF.sort_values('Minutes',  ascending=False)  
      t = "TripDuration(10 min truncated) vs avg mile on hour: " + str(hr)
      CitiAvgDF.plot(x='Minutes', y='Miles',linestyle='--', marker='o', color='r', kind='line',grid=True, title=t)
      f = "citi_by_hour_"+str(hr)+".png"
      plt.savefig(f) 

# for each citibike there is df, union them all
def get_one_citi():
   citi = read_citibike_to_dataframe()
   s =None
   for i in citi:
      if s == None:
         print('start')
         s = i
      else:
         if i!=None:
            s = s.unionAll(i)
   return s
# get combined dataframe of all
citi =get_one_citi()
#df = get_single()
save_plot_by_hour(citi)
#df.createOrReplaceTempView("citibike")
#results = spark.sql("SELECT * FROM citibike")
#results.show()
# to store table on hdfs
#comb.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/btimals000/citibike_spark1.csv')


