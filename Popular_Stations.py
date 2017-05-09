import csv
import operator

d = dict()

with open('Latest_Turnstile_Counts.csv', 'rb') as fi:
    reader = csv.DictReader(fi)
    for row in reader:
        if row['STATION'] not in d:
            d[row['STATION']] = [int(row['ENTRIES']), int(row['EXITS'])]
        else:
            d[row['STATION']][0] += int(row['ENTRIES'])
            d[row['STATION']][1] += int(row['EXITS'])

top_entry_stations = sorted(d.items(), key=operator.itemgetter(1))
top_exit_stations = sorted(d.items(), key=operator.itemgetter(2))

# print top 50 stations
print "Top Exit Stations:"
for x in range(50):
        print top_exit_stations[x]

print "Top Entry Stations: "
for x in range(50):
    print top_entry_stations[x]


