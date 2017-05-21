
# coding: utf-8

# In[9]:

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
from pyspark.sql.functions import col, avg


# In[14]:

def get_citi_schema():
    field_name = ['start_latitude','start_longitude', 'vendor_id', 'end_latitude', 'end_longitude','starttime','duration']   
    field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), IntegerType(),IntegerType()]
    # create schema
    field=[]
    for i in range(0,7):
        field.append(StructField(field_name[i], field_type[i]))
    schema = StructType(field)# get next month
    return schema


# In[15]:

def next_month(start):
    month = start.month
    while month == start.month:
        start+=timedelta(days = 1)
    return start


# In[18]:

#parse citi csv
def parseCITIBIKECSV(idx, part):
    if(idx==0):
        part.next()
    for line in part:
        row = line.split(',')
        try:
            duration_bucket = int(float(row[0][1:-1]))
            date = row[1][1:-1].split(' ')
            hour = date[1].split(':')[0]
            #if hour >= 7 and hour <= 9:
            yield (float(row[5][1:-1]),float(row[6][1:-1]),int(row[7][1:-1]),float(row[9][1:-1]),float(row[10][1:-1]),int(hour), (int(duration_bucket/60)))
        except:
            continue


# In[19]:

def get_miles(part):
    start = (part[0], part[1])
    end = (part[3], part[4])
    m = vincenty(start,end).miles
    return (part[6], m)


# In[20]:

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


# In[21]:

def get_single():
    cur = '/user/gdicarl000/projectdata/citibike/201307-citibike-tripdata.csv'
    r = sc.textFile(cur).cache()
    c_single = r.mapPartitionsWithIndex(parseCITIBIKECSV)
    schema = get_citi_schema()
    df = sqlContext.createDataFrame(c_single,schema)
    return df


# In[33]:

def strip_locations(row):
    point_1 = (40.7047177,-74.00926027)  #"Pearl St & Hanover Square"
    point_2 = (40.75513557,-73.98658032) # Broadway & W 41 S  
    start = (row[0], row[1]) 
    end = (row[3],row[4])  
    d0 = vincenty(start,point_1).miles  
    d1 = vincenty(start,point_2).miles  
    if(d0 >0.10 and d1> 0.10): # 0.1 miles filter
        return False
    d2 = vincenty(end,point_1).miles
    d3 = vincenty(end,point_2).miles
    if(d2 >0.10 and d3>0.10): # 0.1 miles filter
        return False
    return True


# In[22]:

def get_plot_df(df, hour):
    # for miles
    avgRDD = df.rdd.filter(lambda x: x[5] == hour)
    avgRDD= avgRDD.map(get_miles).filter(lambda x: x[1] >0).mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda y : 1.0 * y[0] / y[1])
    #avgRDD.show(2)
    schema2 = StructType([StructField("Minutes", IntegerType()), StructField("Miles", FloatType())])
    mdf = sqlContext.createDataFrame(avgRDD,schema2)
    #mdf.show(10)
    return mdf


# In[23]:

def save_plot_by_hour(df, title):
    df = df.rdd.filter(strip_locations)
    for hr in (8, 9, 17, 18):
        mdf = get_plot_df(df, hr)
        # convert to panda df
        AvgDF = mdf.toPandas()
        AvgDF=AvgDF.sort_values('Minutes',  ascending=False)  
        t = "Miles per minutes in Hour: "+ str(hr) +" at " + title
        AvgDF.plot(x='Minutes', y='Miles',linestyle='--', marker='o', color='r', kind='line',grid=True, title=t)
        f = title+'_At_hr_'+str(hr)+'.png'
        plt.savefig(f) 
        del AvgDF


# In[ ]:

def get_plot_df_all(prdd, hour):
    # for miles
    avgRDD = prdd
    avgRDD= avgRDD\
        .map(get_miles)\
        .filter(lambda x: x[1] >0)\
        .mapValues(lambda x: (x,1))\
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
        .mapValues(lambda y : 1.0 * y[0] / y[1])
    #avgRDD.show(2)
    schema2 = StructType([StructField("Minutes", IntegerType()), StructField("Miles", FloatType())])
    mdf = sqlContext.createDataFrame(avgRDD,schema2)
    #mdf.show(10)
    return mdf


# In[23]:
def save_plot_by_hour(df, title):
    for hr in (8, 9, 17, 18):
        mdf = get_plot_df(df, hr)
        # convert to panda df
        AvgDF = mdf.toPandas()
        quantile=AvgDF['Minutes'].quantile(.90)
        quantile=AvgDF['Miles'].quantile(.90)
        for index, row in AvgDF.iterrows():
            if (row["Minutes"] >= quantile) or (row["Miles"] >= quantile):
                AvgDF.drop(index, inplace=True)
        AvgDF=AvgDF.sort_values('Minutes',  ascending=False)  
        t = "Miles per minutes in Hour: "+ str(hr) +" at " + title
        AvgDF.plot(x='Minutes', y='Miles',linestyle='--', marker='o', color='r', kind='line',grid=True, title=t)
        f = title+"_by_hour_"+str(hr)+".png"
        plt.savefig(f) 
        del AvgDF

# In[25]:

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


# In[27]:

#parse yellow csv
def parseYELLOWCSV(idx, part):
    if(idx==0):
        part.next()
    for line in part:
        try: 
            row = line.split(',')
            pick = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') #2015-01-08 22:44:09
            drop = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            duration = (drop - pick).total_seconds()/60 
            #if hour >= 7 and hour <= 9:
            yield (float(row[6]), float(row[5]), int(row[0]), float(row[10]), float(row[9]), pick.hour,(int(duration)))
        except:
            continue


# In[27]:

#parse yellow csv
def parseYELLOWCSVTRUNC(idx, part):
    if(idx==0):
        part.next()
    for line in part:
        try: 
            row = line.split(',')
            pick = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') #2015-01-08 22:44:09
            drop = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            duration = (drop - pick).total_seconds()/60 # for each 10 minutes
            #if hour >= 7 and hour <= 9:
            yield (float(row[6]), float(row[5]), int(row[0]), float(row[10]), float(row[9]), pick.hour,(int(duration/10))*10)
        except:
            continue


# In[29]:

def get_yellow_schema():
    field=[]
    field_name = ['start_latitude','start_longitude', 'vendor_id', 'end_latitude', 'end_longitude','starttime','duration']   
    field_type = [FloatType(), FloatType(),IntegerType(), FloatType(), FloatType(), IntegerType(),IntegerType()]
    for i in range(0,7):
        field.append(StructField(field_name[i], field_type[i]))
    schema = StructType(field) 
    return schema


# In[30]:

def read_yellow_to_dataframe():                      
   # create fields to give csv structure
   # create schema
    schema = get_yellow_schema()
    cur = '/user/gdicarl000/projectdata/cardata.csv'
    c2 = sc.textFile(cur).mapPartitionsWithIndex(parseYELLOWCSV)
    df = sqlContext.createDataFrame(c2,schema)
    return df


# In[ ]:

def read_yellow_truncated():
    schema = get_yellow_schema()
    cur = '/user/gdicarl000/projectdata/cardata.csv'
    c2 = sc.textFile(cur).mapPartitionsWithIndex(parseYELLOWCSVTRUNC)
    df = sqlContext.createDataFrame(c2,schema)
    return df


# In[36]:

from pyspark.sql.functions import mean
def get_avg_by_hr(df):
    df = df.rdd.filter(strip_locations)
    schema2 = StructType([StructField("Minutes", IntegerType()), StructField("Miles", FloatType())])
    for hr in (8, 9, 17, 18):
        avgRDD = df.filter(lambda x: x[5] == hr)
        avgRDD= avgRDD\
            .map(get_miles)\
            .filter(lambda x: x[1] >0)\
            .mapValues(lambda x: (x,1))\
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
            .mapValues(lambda y : 1.0 * y[0] / y[1])
        mdf = sqlContext.createDataFrame(avgRDD,schema2)
        mdf.createOrReplaceTempView("TEMP")
        result = spark.sql("SELECT sum(Minutes), sum(Miles) FROM TEMP")
        print hr
        result.show()


# In[ ]:

yellow = read_yellow_to_dataframe()
yellow_t = read_yellow_truncated()
citi =get_one_citi()
save_plot_by_hour(citi, "CITI_Pearl_Broadway")
save_plot_by_hour(yellow, "YellowCab_pearl_Broadway")
#save_plot_by_hour_overall(yellow_t, "YellowCab_Front_Broadway_OverAll")
#get_avg_by_hr(citi)
#get_avg_by_hr(yrllow)

