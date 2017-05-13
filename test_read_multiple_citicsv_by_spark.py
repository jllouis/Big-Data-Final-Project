#run in pyspark not spark-submit, can do that too but need to add some lines

from pyspark import SparkContext
from pyspark.sql import SQLContext
from datetime import datetime
from datetime import timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import csv

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

'''0) tripduration,   => 634
   1) starttime,      => 2013-07-01 00:00:00
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
    try:
        for row in csv.reader(part):
            try:
                if len(row)!=15:
                    continue
                dates,time = row[1].split(' ')
                hours,minutes,seconds = time.split(':')
                if hours.isdigit() and int(hours) >= 7 and int(hours) <= 9:
                    yield (row[5],row[6],row[7],row[9],row[10],row[1])
            except:
                continue
    except csv.Error:
        part.next()
    
        
            

# create rdd, read from citibike files
# map rdd to a schema we want
# define that schema
# convert rdd to rdd using that scmema
def read_citibike_to_dataframe():                      
    end = datetime.strptime('201410', '%Y%m')
    start = datetime.strptime('201307', '%Y%m')
    # create fields to give csv structure
    field_name = ['start_latitude','start_longitude', 'end_id', 'end_latitude', 'end_longitude','starttime']   
    field_type = [StringType(), StringType(),StringType(), StringType(), StringType(), StringType()]
    # create schema
    field=[]
    for i in field_name:
        field.append(StructField(i, StringType()))
    schema = StructType(field)
    while start <= end:
        try:
            cur = '/user/gdicarl000/projectdata/citibike/'+start.strftime('%Y%m')+'-citibike-tripdata.csv'
            c2 = sc.textFile(cur).mapPartitionsWithIndex(parseCITIBIKECSV)
            df = sqlContext.createDataFrame(c2,schema)
            start= next_month(start)
            print cur
            yield df
        except Py4JJavaError as e:
            print "error"
        

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
                #i.createOrReplaceTempView("citibike")
                #results = spark.sql("SELECT count(*) FROM citibike")
                #print(results.show())
    return s

# get combined dataframe of all
comb =get_one_citi()
comb.createOrReplaceTempView("citibike")
results = spark.sql("SELECT count(*) FROM citibike")
print(results.show())

#store table
#comb.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save('/user/btimals000/citibike_spark1.csv')


###########################
#for single file for  testing
# create fields to give csv structure
'''field_name = ['start_latitude','start_longitude', 'end_id', 'end_latitude', 'end_longitude','starttime']   
field_type = [StringType(), StringType(),StringType(), StringType(), StringType(), StringType()]
    
# create schema
field=[]
for i in range(0,5):
    field.append(StructField(field_name[i], field_type[i])) 
schema = StructType(field)

field_name = ['start_latitude','start_longitude', 'end_id', 'end_latitude', 'end_longitude','starttime']   
field_type = [StringType(), StringType(),StringType(), StringType(), StringType(), StringType()]
cur = '/user/gdicarl000/projectdata/citibike/201307-citibike-tripdata.csv'
c_single = sc.textFile(cur).mapPartitionsWithIndex(parseCITIBIKECSV)
df = sqlContext.createDataFrame(c_single,schema)'''


