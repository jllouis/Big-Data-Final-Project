 #!/usr/bin/python
import sys

trip_info = []
for row in sys.stdin:
   if row[0] == "tripduration":
       next(row)
   row = row.split(',')
   dates,time = row[1].split(' ')
   hours,minutes,seconds = time.split(':')
   if int(hours) >= 7 and int(hours) <= 9:
        tripduration = row[0]
        startlongitude = row[5]
        startlatitude = row[6]
        stoplatitude = row[8]
        stoplongitude =row[9]
        starttime = row[1]
        trip_info.append((tripduration, startlongitude,startlatitude,stoplatitude,stoplongitude,starttime))
        sys.stdout.write(''.join(str(a) for a in trip_info))

sys.stdout.flush()
