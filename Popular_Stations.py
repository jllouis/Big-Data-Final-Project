import csv
import operator

d = dict()
numStations = 0

with open('Latest_Turnstile_Counts_At_Midnight.csv', 'rb') as fi:
    reader = csv.DictReader(fi)
    for row in reader:
        if row['STATION'] not in d:
            d[row['STATION']] = [int(row['ENTRIES']), int(row['EXITS']), str(row['LINENAME'])]
            numStations += 1
        else:
            d[row['STATION']][0] += int(row['ENTRIES'])
            d[row['STATION']][1] += int(row['EXITS'])

top_entry_stations = sorted(d.items(), key=operator.itemgetter(0), reverse=True)
top_exit_stations = sorted(d.items(), key=operator.itemgetter(1), reverse=True)
print "Number of Stations: " + str(numStations)

# print top 50 stations
print "Top Exit Stations:"
for x in range(50):
        print top_exit_stations[x]

print "Top Entry Stations: "
for x in range(50):
    print top_entry_stations[x]


