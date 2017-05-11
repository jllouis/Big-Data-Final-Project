import csv
import operator

station_totals = dict()
numStations = 0
unmatched = 0

with open('Latest_Turnstile_Counts_At_Midnight.csv', 'rb') as fi:
    reader = csv.DictReader(fi)
    for row in reader:
        if str(row['STATION'])[:5] not in station_totals:
            station_totals[str(row['STATION'])[:5]] = [int(row['ENTRIES']), int(row['EXITS']),
                                                       str(row['LINENAME']), None]
            numStations += 1
        else:
            station_totals[str(row['STATION'])[:5]][0] += int(row['ENTRIES'])
            station_totals[str(row['STATION'])[:5]][1] += int(row['EXITS'])

station_locations = dict()

with open('DOITT_SUBWAY_STATION_01_13SEPT2010.csv') as fi:
    reader = csv.DictReader(fi)
    for record in reader:
        station_locations[str(record['NAME']).upper()[:5]] = str(record['the_geom'])[7:-1].replace(' ', ', ')

    # noinspection PyRedeclaration
    for station in station_locations:
        station = str(station).replace('TH', '')
        station = str(station).replace('ND', '')

for station in station_totals:
    if station in station_locations:
        station_totals[station] = [station_totals[station][0], station_totals[station][1],
                                   station_totals[station][2], station_locations[station]]
    else:
        unmatched += 1

print "Was unable to match " + str(unmatched) + " stations."

top_entry_stations = sorted(station_totals.items(), key=operator.itemgetter(0), reverse=True)
# top_exit_stations = sorted(station_totals.items(), key=operator.itemgetter(1), reverse=True)


print "Number of Stations: " + str(numStations)

# remove stations without coordinates

for i in range(136):
    if top_entry_stations[i][1][3] is None:
        del top_entry_stations[i]

top_entry_stations_reversed = []

for i in range(99, -1, -1):
    top_entry_stations_reversed.append(top_entry_stations[i])

# Commented out the following code because I figured the most popular entry and exist stations
# would have much overlap since people usually use the same station to exit to work, and commute to home
# for i in range(0, 100):
#     if top_exit_stations[i][1][3] is None:
#         del top_exit_stations[i]
#
# # print top 50 stations
# print "Top Exit Stations:"
# for x in range(100):
#         print top_exit_stations[x]

print len(top_entry_stations_reversed)
print len(top_entry_stations)
with open('Popular_Stations_Output.csv', 'wb') as fi:
    writer = csv.writer(fi, delimiter=',')
    writer.writerow(['START STATION', 'START LONGITUDE', 'START LATITUDE',
                     'END STATION', 'END LONGITUDE', 'END LATITUDE'])
    for x in range(100):
        writer.writerow([top_entry_stations[x][0], top_entry_stations[x][1][3].split(', ')[1],
                         top_entry_stations[x][1][3].split(', ')[0],
                         top_entry_stations_reversed[x][0],
                         top_entry_stations_reversed[x][1][3].split(', ')[1],
                         top_entry_stations_reversed[x][1][3].split(', ')[0]])

print('Done!')
