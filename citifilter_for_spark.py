import csv

# parse citi csv
def parseCITIBIKECSV(idx, part):
    if(idx==0):
        part.next()
    for row in csv.reader(part):
        dates,time = row[1].split(' ')
        hours,minutes,seconds = time.split(':')
        if int(hours) >= 7 and int(hours) <= 9:
         yield 

count = 0
for row in sys.stdin:
   if count == 0:
        count = 1
   else:
        row = row.split(',')
        dates,time = row[1].split(' ')
        hours,minutes,seconds = time.split(':')
        if int(hours) >= 7 and int(hours) <= 9:
             sys.stdout.write(row[5] +',')
             sys.stdout.write(row[6]+',')
             sys.stdout.write(row[7]+',')
             sys.stdout.write(row[9]+',')
             sys.stdout.write(row[10]+',')
             sys.stdout.write(row[1]+',')


sys.stdout.flush()
