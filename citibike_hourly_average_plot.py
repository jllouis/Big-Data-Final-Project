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

# get next month
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
      date = row[1][1:-1].split(' ')
      hour = date[1].split(':')[0]
      #if hour >= 7 and hour <= 9:
      yield (float(row[5][1:-1]),float(row[6][1:-1]),int(row[7][1:-1]),float(row[9][1:-1]),float(row[10][1:-1]),int(hour))
         
def get_miles(part):
   start = (part[0], part[1])
   end = (part[3], part[4])
   m = vincenty(start,end).miles
   return (part[5], m)
   

# create rdd, read from citibike files
# map rdd to a schema we want
# define that schema
# convert rdd to rdd using that scmema
def read_citibike_to_dataframe():                      
   end = datetime.strptime('201703', '%Y%m')
   start = datetime.strptime('201307', '%Y%m')
   # create fields to give csv structure
   field_name = ['start_latitude','start_longitude', 'end_id', 'end_latitude', 'end_longitude','starttime']   
   field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), StringType()]
   # create schema
   field=[]
   for i in range(0,6):
      print i
      field.append(StructField(field_name[i], field_type[i]))
   schema = StructType(field)
   while start < end:
      cur = '/user/gdicarl000/projectdata/citibike/'+start.strftime('%Y%m')+'-citibike-tripdata.csv'
      c2 = sc.textFile(cur).mapPartitionsWithIndex(parseCITIBIKECSV)
      df = sqlContext.createDataFrame(c2,schema)
      start= next_month(start)
      #df.show(10)
      print cur
      #print c2.count()
      yield df
        

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
comb =get_one_citi()
#comb.createOrReplaceTempView("citibike")
#results = spark.sql("SELECT count(*) FROM citibike")
#print(results.show())

#store table
#comb.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/btimals000/citibike_spark1.csv')

# for miles
avgRDD = comb.rdd.map(get_miles)
avgRDD= avgRDD.mapValues(lambda x: (x,1))
avgRDD = avgRDD.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avgRDD= avgRDD.mapValues(lambda y : 1.0 * y[0] / y[1])
#avgRDD.take(2)

schema2 = StructType([StructField("Hour", IntegerType()), StructField("Miles", FloatType())])
mdf = sqlContext.createDataFrame(avgRDD,schema2)
#mdf.show(10)

# convert to pdf
mpdDF = mdf.toPandas()#pdf = pdf.cumsum()
#mpdDF['Time'] = pd.to_datetime(mpdDF.Epoc_Time,unit='s')

mpdDF.plot(x='Hour', y='Miles', kind='scatter')
plt.savefig('miles_avg.png')


###########################
#for single file for  testing
# create fields to give csv structure
'''
field_name = ['start_latitude','start_longitude', 'end_id', 'end_latitude', 'end_longitude','starttime']   
field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), IntegerType()]
# create schema
field=[]
for i in range(0,6):
   print i
   field.append(StructField(field_name[i], field_type[i]))
        
schema = StructType(field)
cur = '/user/gdicarl000/projectdata/citibike/201307-citibike-tripdata.csv'
c_single = sc.textFile(cur).mapPartitionsWithIndex(parseCITIBIKECSV)
df = sqlContext.createDataFrame(c_single,schema)
df.show(10)
mdf = df.mapPartitionsWithIndex(get_miles)
df.createOrReplaceTempView("citibike")
results = spark.sql("SELECT * FROM citibike")
print(results.show())



###########
#print single for test

# convert to pdf
#pdDF = df.toPandas()#pdf = pdf.cumsum()
#pdDF.plot(x='starttime', y='start_latitude')
#plt.savefig('foo.png')

cur = '/user/gdicarl000/projectdata/citibike/201307-citibike-tripdata.csv'
c_single = sc.textFile(cur).mapPartitionsWithIndex(parseCITIBIKECSV)
df = sqlContext.createDataFrame(c_single,schema)
'''
