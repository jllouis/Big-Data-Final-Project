import csv

from geopy.distance import vincenty

# parser = OptionParser(version="%prog 0.1")
# parser.add_option("-f", "--file", dest="filename", metavar="INPUT_FILE_NAME",
#               help="The input file to process.")
# parser.add_option("-o", "--outfile", dest="output", metavar="OUTPUT_FILE_NAME",
#               help="The name of the output file.")

# (options, args) = parser.parse_args()

for iterator in range(0, 9):
    with open("Uber_Time_Estimates_"+str(iterator)+".csv", 'rb') as in_file:
        with open("uber_speed_calculation_output_"+str(iterator)+".csv", 'wb') as out_file:
            reader = csv.DictReader(in_file)
            writer = csv.writer(out_file, delimiter=',')
            writer.writerow(['TIMESTAMP', 'START STATION', 'START LONGITUDE', 'START LATITUDE',
                             'END STATION', 'END LONGITUDE', 'END LATITUDE', 'TIME ESTIMATE',
                             'SPEED (MPH)'])
            for row in reader:
                writer.writerow([row['TIMESTAMP'], row['START STATION'], row['START LONGITUDE'],
                                 row['START LATITUDE'], row['END STATION'], row['END LONGITUDE'],
                                 row['END LATITUDE'], row['TIME ESTIMATE'],
                                (float(str(vincenty((row['START LATITUDE'], row['START LONGITUDE']),
                                                    (row['END LATITUDE'], row['END LONGITUDE'])))[:-3])
                                 / float(row['TIME ESTIMATE'])) * 60])
