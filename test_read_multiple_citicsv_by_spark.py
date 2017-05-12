from datetime import datetime  
from datetime import timedelta

def next_month(start):
    month = start.month
    while month == start.month:
        start+=timedelta(days = 1)
    return start


def read_citibike_to_dataframe():
    end = datetime.strptime('201612', '%Y%m')
    start = datetime.strptime('201307', '%Y%m')
    while start <= end:
        cur = '/user/gdicarl000/projectdata/citibike/'+start.strftime('%Y%m')+'-citibike-tripdata.csv'
        print cur
        c = sqlContext.read.format("csv").option("delimiter",",").option("quote","").option("inferSchema", "true").load(cur)
        start= next_month(start)
        yield c
   

#

def get_one_citi():   
    citi = read_citibike_to_dataframe()
    s =None
    for i in citi:
        if s == None:
            s = i
        else:
            s.unionAll(i)
    return s
if __name__=='__main__':
    sc = SparkContext()
    c =get_one_citi()
    c.registerTempTable('test')
    c.write.format('com.databricks.spark.csv').save('/user/gdicarl000/projectdata/citibike/temp_spark_combine')
