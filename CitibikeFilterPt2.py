 #!/usr/bin/python
import sys

for row in sys.stdin:
    dates,time = row[3].split(' ')
    hours,minutes,seconds = time.split(':')
    if int(hours) >= 7 and int(hours) <= 9:
        tripduration = row[2]
        longitude = row[7]
        latitude = row[6]
        starttime = row[3]
        trip_info.append((tripduration, longitude,latitude,starttime))
        sys.stdout.write(trip_info)