 #!/usr/bin/python
import sys

count = 0
for row in sys.stdin:
   if count == 0:
        count = 1
   else:
        row = row.split(',')
        dates,time = row[1].split(' ')
        hours,minutes,seconds = time.split(':')
        if int(hours) >= 7 and int(hours) <= 9:
             sys.stdout.write(row[0])
             sys.stdout.write(row[6])
             sys.stdout.write(row[7])
             sys.stdout.write(row[9])
             sys.stdout.write(row[10])
             sys.stdout.write(row[1])


sys.stdout.flush()
