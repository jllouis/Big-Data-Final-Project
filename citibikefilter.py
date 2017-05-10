#!/usr/bin/python
import csv
import sys


citbike_files = sys.argv[1]

for file in citbike_files:
    def csvRows(filename):
        with open(filename, 'r') as fi:
            reader = csv.DictReader(fi)
            for row in reader:
                yield row
    
trip_info = []            
for row in csvRows(file):
    dates, time = row['starttime'].split(' ')
    hours,minutes,seconds = time.split(':')
    if int(hours) >= 7 and int(hours) <= 9 :
        tripduration = row['tripduration']
        longitude = row['start_station_longitude']
        latitude = row['start_station_latitude']
        starttime = row['starttime']
        trip_info.append((tripduration, longitude,latitude,starttime))

print trip_info
