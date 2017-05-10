import csv
import operator

station_totals = dict()
numStations = 0

with open('Latest_Turnstile_Counts_At_Midnight.csv', 'rb') as fi:
    reader = csv.DictReader(fi)
    for row in reader:
        if row['STATION'] not in station_totals:
            station_totals[row['STATION']] = [int(row['ENTRIES']), int(row['EXITS']),
                                              str(row['LINENAME']), None]
            numStations += 1
        else:
            station_totals[row['STATION']][0] += int(row['ENTRIES'])
            station_totals[row['STATION']][1] += int(row['EXITS'])

# station_locations = dict()
#
# with open('DOITT_SUBWAY_STATION_01_13SEPT2010.csv') as fi:
#     reader = csv.DictReader(fi)
#     for record in reader:
#         station_locations[record['NAME']] = record['the_geom']
#
# for station in station_totals:
#     station_totals[station] = [station_totals[station][0], station_totals[station][1],
#                                station_totals[station][2], station_locations[station]]

top_entry_stations = sorted(station_totals.items(), key=operator.itemgetter(0), reverse=True)
top_exit_stations = sorted(station_totals.items(), key=operator.itemgetter(1), reverse=True)


print "Number of Stations: " + str(numStations)

# print top 50 stations
print "Top Exit Stations:"
for x in range(50):
        print top_exit_stations[x]

print "Top Entry Stations: "
for x in range(50):
    print top_entry_stations[x]


