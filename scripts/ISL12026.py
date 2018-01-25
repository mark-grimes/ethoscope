import smbus
import time
from datetime import datetime

# Class to talk to the ISL1206 RTC using I2C protocol.
# Written by P. Baesso - RYMAPT LTD - 2017

class ISL12026:
# Define the SMBus interface
	def __init__(self, bus):
		#bus = smbus.SMBus(1)
		self.bus= bus
		self.DEVICE_ADDRESS = 0x6F      #7 bit address (will be left shifted to add the read write bit)
		self.DATE_LIST= ['SEC', 'MIN', 'HR', 'DATE', 'MONTH', 'YR', 'DAY', 'Y2K']


	def __parseDateList(self, datetimeList):
		#Takes a list containing timestamp values and returns a datetime object.
		formattedTimestamp=[-1, -1, -1, -1, -1, -1]
		formattedTimestamp[5]= self.__parseReg(datetimeList[0])
		formattedTimestamp[4]= self.__parseReg(datetimeList[1])
		formattedTimestamp[3]= self.__parseHour(datetimeList[2])
		formattedTimestamp[2]= self.__parseReg(datetimeList[3])
		formattedTimestamp[1]= self.__parseReg(datetimeList[4])
		formattedTimestamp[0]= 100*(self.__parseReg(datetimeList[7])) + (self.__parseReg(datetimeList[5]))
		myDate= datetime(formattedTimestamp[0], formattedTimestamp[1], formattedTimestamp[2], formattedTimestamp[3], formattedTimestamp[4], formattedTimestamp[5])
		return myDate

	def __parseDateObj(self, datetimeObj):
		#Takes a python datetime object and returns a list that can be written into the RTC chip.
		y2k= (datetimeObj.year)//100
		yr= (datetimeObj.year) % 100
		month= datetimeObj.month
		day= datetimeObj.day
		hour= datetimeObj.hour
		minute= datetimeObj.minute
		second= datetimeObj.second
		weekday= datetimeObj.weekday()
		outList= [second, minute, hour, day, month, yr, weekday, y2k]
		print y2k, yr, month, day, hour, minute, second, weekday
		return outList

	def __parseReg(self, inChar):
		units= inChar & 0x0F
		decs= ((inChar & 0xF0) >> 4)*10
		outValue= units+decs
		return outValue


	def __parseDec(self, inDec):
		# Transoform an int [0, 99] into two nibbles and merge them into a char
		# For instance dec(93) becomes 0x93
		hiNibble= inDec // 10
		lowNibble= inDec % 10
		hiNibble= hiNibble << 4
		outChar= hiNibble | lowNibble
		return outChar



	def __parseHour(self, inChar):
		if (self.__get_bit(inChar, 7)):
			#print "24h time"
			outChar= inChar & 0x7F
			outChar= self.__parseReg(outChar)
		else:
			#print "12h time"
			if (self.__get_bit(inChar, 5)):
				outChar= 12
				#print "PM"
			else:
				outChar=0
				#print "AM"
			inChar = inChar & 0x1F
			inChar= self.__parseReg(inChar)
			outChar= outChar+ inChar
		return outChar


	def __get_bit(self, byteval,idx):
		return ((byteval&(1<<idx))!=0);


	def getReg(self, regAdd):
		# Get content of specific RTC register
		self.bus.write_byte_data(self.DEVICE_ADDRESS, 0x00, regAdd)
		myData = self.bus.read_byte(self.DEVICE_ADDRESS)
		return myData


	def setReg(self, regAdd, regData):
		# Write content of specific RTC register. regData can be a char or a list of chars.
		sendChar= regData
		sendChar.insert(0, regAdd)
		#for iData in sendChar:
		#	print hex(iData)
		self.bus.write_i2c_block_data(self.DEVICE_ADDRESS,0x00,[0x3F, 0x02]) #write 0x02 to SR to set WEL
		self.bus.write_i2c_block_data(self.DEVICE_ADDRESS,0x00,[0x3F, 0x06]) #write 0x06 to SR to set WEL and RWEL
		self.bus.write_i2c_block_data(self.DEVICE_ADDRESS,0x00,sendChar)


	def getStatus(self):
		# Return the statur of the RTC register. If bit 0 is set, the chip had a power loss and the time is
		# likely wrong. Bit 4 set indicates a failure in the oscillator. Bit 7 set indicates the device is
		# powered by the battery.
		statusReg= self.getReg(0x3F)
		return statusReg


	def getDate(self):
		# Read the date from the RTC and return it as a datetime object.
		myDateTime=[]
		for iData in range(0,8):
			self.bus.write_byte_data(self.DEVICE_ADDRESS, 0x00, 0x30+iData) # the 0x00, 0x00 is the '16 bit' address split into 2 bytes
			myData = self.bus.read_byte(self.DEVICE_ADDRESS) # this will read at the current address pointer, which we on the previous line
			myDateTime.append(myData)
			#print self.DATE_LIST[iData], self.__parseReg(myData)
		formattedTimestamp= self.__parseDateList(myDateTime)
		return formattedTimestamp

	def setDate(self, datetimeObj):
		# Parse a datetime object and write its content in the RTC
		datetimeList=self.__parseDateObj(datetimeObj)

		datetimeList[0]= self.__parseDec(datetimeList[0])
		datetimeList[1]= self.__parseDec(datetimeList[1])
		datetimeList[2]= self.__parseDec(datetimeList[2])
		datetimeList[2]= datetimeList[2] | 0x80
		datetimeList[3]= self.__parseDec(datetimeList[3])
		datetimeList[4]= self.__parseDec(datetimeList[4])
		datetimeList[5]= self.__parseDec(datetimeList[5])
		datetimeList[6]= self.__parseDec(datetimeList[6])
		datetimeList[7]= self.__parseDec(datetimeList[7])
		#for iData in datetimeList:
		#	print hex(iData)

		self.setReg(0x30, datetimeList)
