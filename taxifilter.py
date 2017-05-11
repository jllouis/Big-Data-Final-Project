 #!/usr/bin/python
import sys
from datetime import datetime

trip_info = []
count = 0
#reading line by line from standard input
for row in sys.stdin:
   if count != 0: #skipping first line (header for file)
      row = row.split(',')
      dates,pickup_time = row[1].split(' ')
      hours,minutes,seconds = pickup_time.split(':')
      if int(hours) >= 7 and int(hours) <= 9:
        dates, dropoff_time = row[2].split(' ')
        FMT = '%H:%M:%S'
        duration = datetime.strptime(dropoff_time, FMT) - datetime.strptime(pickup_time,FMT)
        longitude_pickup = row[5]
        latitude_pickup = row[6]
        longitude_dropoff = row[9]
        latitude_dropoff = row[10]
        sys.stdout.write(longitude_pickup)
        sys.stdout.write(latitude_pickup)
        sys.stdout.write(longitude_dropoff)
        sys.stdout.write(latitude_dropoff)
        sys.stdout.write(str(duration))
        sys.stdout.write(pickup_time)
   count = 1
   
 #  for i in trip_info:
  #      sys.stdout.write(str(i))
   
sys.stdout.flush()
