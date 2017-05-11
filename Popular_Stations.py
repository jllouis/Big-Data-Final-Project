import csv
import operator

station_totals = dict()  # this dictionary keeps track of the total number of entries and exits per station
numStations = 0  # keeps track of the number of stations
unmatched = 0  # keeps track of the number of stations there were unable to be matched with location coordinates
num_chars = 5

with open('Latest_Turnstile_Counts_At_Midnight.csv', 'rb') as fi:
    ''' Insert each station into the dictionary given that it's not already in there
        Tally up the ENTRIES and EXITS count given that it already is in there'''
    reader = csv.DictReader(fi)
    for row in reader:
        if str(row['STATION'])[:num_chars] not in station_totals:  # is the station not already in the dictionary?
            station_totals[str(row['STATION'])[:num_chars]] = [int(row['ENTRIES']), int(row['EXITS']),
                                                               str(row['LINENAME']), None]
            numStations += 1
        else:  # station already is already in dictionary, so just increment the existing totals
            station_totals[str(row['STATION'])[:num_chars]][0] += int(row['ENTRIES'])
            station_totals[str(row['STATION'])[:num_chars]][1] += int(row['EXITS'])

station_locations = dict()  # this dictionary will keep track of stations and their location coordinates

with open('DOITT_SUBWAY_STATION_01_13SEPT2010.csv') as fi:
    ''' Insert stations from the csv file and its coordinates (in useful form) into the dictionary'''
    reader = csv.DictReader(fi)
    for record in reader:
        ''' Just use the first 5 letters as uppercase as the key for the dictionary for easy matching later'''
        station_locations[str(record['NAME']).upper()[:num_chars]] = str(record['the_geom'])[7:-1].replace(' ', ', ')

    # remove 'th' 'rd' 'st' and 'nd' post-fixes from station numbers
    for station in station_locations:
        station = str(station).replace('TH', '').replace('2ND', '2').replace('3RD', '3').replace('1ST', '1')

''' Try to match the stations from the station totals to the ones in station locations to
    get the most popular station's location coordinates. Append coordinate to existing station_totals dictionary '''
for station in station_totals:
    if station in station_locations:
        station_totals[station] = [station_totals[station][0], station_totals[station][1],
                                   station_totals[station][2], station_locations[station]]
    else:  # keep track of how many stations are unmatched
        unmatched += 1

print "Was unable to match " + str(unmatched) + " stations."

''' sort to get the most popular stations based on ENTRIES number '''
top_entry_stations = sorted(station_totals.items(), key=operator.itemgetter(0), reverse=True)
# top_exit_stations = sorted(station_totals.items(), key=operator.itemgetter(1), reverse=True)


print "Number of Stations: " + str(numStations)

# remove stations without coordinates

''' Remove the top stations without location coordinates '''
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

# print len(top_entry_stations_reversed)
# print len(top_entry_stations)

''' output files into a csv, put nth most popular station along side nth least popular station, for Uber processing '''
with open('Popular_Stations_Output.csv', 'wb') as fi:
    writer = csv.writer(fi, delimiter=',')
    writer.writerow(['START STATION', 'START LONGITUDE', 'START LATITUDE',
                     'END STATION', 'END LONGITUDE', 'END LATITUDE'])
    for x in range(100):  # only the top 100 stations
        writer.writerow([top_entry_stations[x][0], top_entry_stations[x][1][3].split(', ')[1],
                         top_entry_stations[x][1][3].split(', ')[0],
                         top_entry_stations_reversed[x][0],
                         top_entry_stations_reversed[x][1][3].split(', ')[1],
                         top_entry_stations_reversed[x][1][3].split(', ')[0]])

print('Done!')
