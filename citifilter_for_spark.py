import csv

# parse citi csv
def parseCITIBIKECSV(idx, part):
    if(idx==0):
        part.next()
    for row in csv.reader(part):
        dates,time = row[1].split(' ')
        hours,minutes,seconds = time.split(':')
        if int(hours) >= 7 and int(hours) <= 9:
            yield (row[5],row[6],row[7],row[9],row[10],row[1])
