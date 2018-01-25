import smbus
import time
from datetime import datetime
from ISL12026 import ISL12026

bus = smbus.SMBus(1)
myRTC= ISL12026(bus)

#GET RTC STATUS (useful to know if power loss)
#print hex(myRTC.getStatus())

#RETRIEVE CURRENT TIME
print myRTC.getDate()

#SET RTC TIME
#datetimeObj= datetime.now()
#myRTC.setDate(datetimeObj)
