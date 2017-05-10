 #!/usr/bin/python
import sys

for row in sys.stdin:
   # dates,time = row[3].split(' ')
   # hours,minutes,seconds = time.split(':')
   sys.stdout.write(row[3])
