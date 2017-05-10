 #!/usr/bin/python
import sys

trip_info = []
for row in sys.stdin:
   if row[0] != "cartodb_id":
       row = row.split(',')
       dates,time = row[3].split(' ')
       hours,minutes,seconds = time.split(':')
       if int(hours) >= 7 and int(hours) <= 9:
            tripduration = row[2]
            longitude = row[7]
            latitude = row[6]
            starttime = row[3]
            trip_info.append((tripduration, longitude,latitude,starttime))
            sys.stdout.write(''.join(str(a) for a in trip_info))

sys.stdout.flush()
