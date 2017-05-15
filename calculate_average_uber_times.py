import csv
from geopy.distance import vincenty


def time_eight():
    total_speed = 0
    total_rows = 0
    total_time = 0
    total_distance = 0
    with open("uber_speed_calculation_output_6.csv", 'rb') as in_file:
        reader = csv.DictReader(in_file)
        for row in reader:
            # print row['TIMESTAMP'][11:13]
            if row['TIMESTAMP'][11:13] == '08':
                total_rows += 1
                total_speed += float(row['SPEED (MPH)'])
                total_time += float(row['TIME ESTIMATE'])
                total_distance += float(str(vincenty((row['START LATITUDE'], row['START LONGITUDE']),
                                                     (row['END LATITUDE'], row['END LONGITUDE'])))[:-3])

    print "8 AM: " + str(total_speed / total_rows)
    print "total time: " + str(total_time)
    print "total distance: " + str(total_distance)

def time_nine():
    total_speed = 0
    total_rows = 0
    total_time = 0
    total_distance = 0
    with open("uber_speed_calculation_output_0.csv", 'rb') as in_file:
        reader = csv.DictReader(in_file)
        for row in reader:
            if row['TIMESTAMP'][11:13] == '09':
                total_rows += 1
                total_speed += float(row['SPEED (MPH)'])
                total_time += float(row['TIME ESTIMATE'])
                total_distance += float(str(vincenty((row['START LATITUDE'], row['START LONGITUDE']),
                        (row['END LATITUDE'], row['END LONGITUDE'])))[:-3])

    print "9 AM: " + str(total_speed / total_rows)
    print "total time: " + str(total_time)
    print "total distance: " + str(total_distance)

def time_five():
    total_speed = 0
    total_rows = 0
    total_time = 0
    total_distance = 0
    with open("uber_speed_calculation_output_2.csv", 'rb') as in_file:
        reader = csv.DictReader(in_file)
        for row in reader:
            if row['TIMESTAMP'][11:13] == '17':
                total_rows += 1
                total_speed += float(row['SPEED (MPH)'])
                total_time += float(row['TIME ESTIMATE'])
                total_distance += float(str(vincenty((row['START LATITUDE'], row['START LONGITUDE']),
                                                     (row['END LATITUDE'], row['END LONGITUDE'])))[:-3])

    print "5 PM: " + str(total_speed / total_rows)
    print "total time: " + str(total_time)
    print "total distance: " + str(total_distance)

def time_six():
    total_speed = 0
    total_rows = 0
    total_time = 0
    total_distance = 0
    with open("uber_speed_calculation_output_3.csv", 'rb') as in_file:
        reader = csv.DictReader(in_file)
        for row in reader:
            if row['TIMESTAMP'][11:13] == '18':
                total_rows += 1
                total_speed += float(row['SPEED (MPH)'])
                total_time += float(row['TIME ESTIMATE'])
                total_distance += float(str(vincenty((row['START LATITUDE'], row['START LONGITUDE']),
                                                     (row['END LATITUDE'], row['END LONGITUDE'])))[:-3])

    print "6 PM: " + str(total_speed / total_rows)
    print "total time: " + str(total_time)
    print "total distance: " + str(total_distance)

time_eight()
time_nine()
time_five()
time_six()