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
            d[row['STATION']][0] += int(row['EXITS'])

# top_stations = sorted(d.items(), key=operator.itemgetter(1))
#
# # print top 50 stations
# print "printing top stations:"
# for x in range(10):
#         print top_stations[x]

