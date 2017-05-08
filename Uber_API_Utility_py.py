# Big Data Project: Uber API Utility Script Version 0.2
# CSC 599, Team 8.
# Takes in csv file of starting longitude and latitude,
# ending longitude and latitude, and outputs a file of the time estimates
# next to the corresponding longitudes and latitudes
from optparse import OptionParser
from uber_rides.session import Session
from uber_rides.client import UberRidesClient
import csv

print "Uber API Utility Script:"


# Parse command line options and arguments, then start process()
def parse_opts():
    # creating and configuring command line options parser
    parser = OptionParser(version="%prog 0.2")
    parser.add_option("-f", "--file", dest="filename", metavar="INPUT_FILE_NAME",
              help="The input file to process.")
    parser.add_option("-o", "--outfile", dest="output", metavar="OUTPUT_FILE_NAME",
              help="The name of the output file.")
    '''
    parser.add_option("-a", "--append", dest="append", default=False, metavar="True/False",
              help="Appends time estimates to the corresponding lines of the input file. *NOT YET IMPLEMENTED*")
    '''
    parser.add_option("-n", "--accuracy", dest="acc", metavar="NUMBER", type="int", default=3,
              help="Number of digits after the decimal place to consider.")

    (options, args) = parser.parse_args()

    # in case of no arguments
    if len(args) == 0:
        parser.print_help()

    process(options.filename, options.output, options.acc)


# creating new session on Ubers Server
def create_uber_session():
    print "Creating Uber Server Session..."
    session = Session(server_token="6aaL6T4Xb22J8gx8FWm-H_MqBoKlWhyjksyD4uNx")
    client = UberRidesClient(session)

    return client


# actually query server for time estimate given start and ending locations
def get_time_estimate(client, start_lat, start_long, end_lat, end_long, seats=2):
    response = client.get_price_estimates(
        start_latitude=start_lat,
        start_longitude=start_long,
        end_latitude=end_lat,
        end_longitude=end_long,
        seat_count=seats
    )

    estimate = response.json.get('prices')
    return estimate[0]['duration'] / 60


# does reads input coordinates, gets time estimates, and writes the coordinates and time estimate in output file
def process(infile, outfile, accuracy=3):
    session_client = create_uber_session()
    print "Opening Input File..."

    with open(infile, 'rb') as readfile:
        reader = csv.reader(readfile, delimiter=',')
        print "Creating Output File..."
        with open(outfile, 'wb') as writefile:
            writer = csv.writer(writefile, delimiter=',')
            print "Processing File..."
            for line in reader:
                writer.writerow([line[0], line[1], line[2], line[3],
                                get_time_estimate(session_client, round(float(line[0]), accuracy),
                                                  round(float(line[1]), accuracy), round(float(line[2]), accuracy),
                                                  round(float(line[3]), accuracy))])


parse_opts()
print "Done."