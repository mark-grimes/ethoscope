import smbus
import time
import easyi2c

# Define the SMBus interface
bus = smbus.SMBus(1)
DEVICE_ADDRESS = 0x6F      #7 bit address (will be left shifted to add the read write bit)
myDevice= easyi2c.IIC(DEVICE_ADDRESS, 1) #address and bus
#DEVICE_REG_MODE1 = 0x00
#DEVICE_REG_LEDOUT0 = 0x1d

#Write a single register
#bus.write_byte_data(DEVICE_ADDRESS, DEVICE_REG_MODE1, 0x80)

#Write an array of registers
#ledout_values = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
#bus.write_i2c_block_data(DEVICE_ADDRESS, DEVICE_REG_LEDOUT0, ledout_values)

###THIS WORKS
#easyi2c
#myData=myDevice.i2c([0x00,0x3F],1)
#for iChar in myData:
#	print hex(iChar)

###THIS WORKS
#SMBus
#bus.write_byte_data(DEVICE_ADDRESS, 0x00, 0x30) # the 0x00, 0x00 is the '16 bit' address split into 2 bytes
#myData = bus.read_byte(DEVICE_ADDRESS) # this will read at the current address pointer, which we on the previous line
#print hex(myData)

#Check status
bus.write_byte_data(DEVICE_ADDRESS, 0x00, 0x3F) # the 0x00, 0x00 is the '16 bit' address split into 2 bytes
myData = bus.read_byte(DEVICE_ADDRESS) # this will read at the current address pointer, which we on the previous line
print 'Status', hex(myData)

#Write current date/time
#bus.write_i2c_block_data(DEVICE_ADDRESS,0x00,[0x3F, 0x02]) #write 0x02 to SR to set WEL
#bus.write_i2c_block_data(DEVICE_ADDRESS,0x00,[0x3F, 0x06]) #write 0x06 to SR to set WEL and RWEL
#bus.write_i2c_block_data(DEVICE_ADDRESS,0x00,[0x30, 0x45, 0x22, 0x91, 0x04, 0x07, 0x17, 0x03, 0x20]) #write to RTC registers

#Read current date/time
myDateTime=[]
for iData in range(0,8):
	bus.write_byte_data(DEVICE_ADDRESS, 0x00, 0x30+iData) # the 0x00, 0x00 is the '16 bit' address split into 2 bytes
	myData = bus.read_byte(DEVICE_ADDRESS) # this will read at the current address pointer, which we on the previous line
	myDateTime.append(myData)
	print 'Reg', hex(0x30+iData), hex(myData)
#print myDateTime
