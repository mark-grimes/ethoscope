#! /usr/bin/env python

"""
Tests for the HIH6130 class which talks to the HIH6130 temperature and humidity sensor.
"""

__author__    = "Paolo Baesso, Mark Grimes"
__copyright__ = "Copyright 2017, Rymapt Ltd"
__license__   = "MIT"
# MIT licence is available at https://opensource.org/licenses/MIT

import unittest
import time
import ethoscope.hardware.input.easyi2c as easyi2c
from ethoscope.hardware.input.HIH6130 import HIH6130, BufferedHIH6130

class TestHIH6130(unittest.TestCase):
    def test_constructWitheasyi2c(self):
        DEVICE_ADDRESS= 0x27 #For temperature sensor
        easybus= easyi2c.IIC(DEVICE_ADDRESS, 1)
        myHIH = HIH6130(easybus)
        self.performTests(myHIH)

    def test_constructAutomatically(self):
        myHIH = HIH6130()
        self.performTests(myHIH)

    def performTests(self, device):
        data = device.getData()
        self.assertEqual(len(data), 3)
        # Test the status bits
        self.assertEqual(data[0], 0)
        # Test the relative humidity.
        self.assertGreaterEqual(data[1], 0)
        self.assertLessEqual(data[1], 100)
        # Test the temperature. These temperatures are taken from the documented
        # operating temperature, so if you're testing when the temperature is outside
        # this range the chip probably won't work anyway.
        self.assertGreaterEqual(data[2], -25)
        self.assertLessEqual(data[2], 85)

class TestBufferedHIH6130(unittest.TestCase):
    def test_constructWitheasyi2c(self):
        DEVICE_ADDRESS= 0x27 #For temperature sensor
        easybus= easyi2c.IIC(DEVICE_ADDRESS, 1)
        myHIH = BufferedHIH6130(easybus)
        self.performTests(myHIH)

    def test_constructAutomatically(self):
        myHIH = BufferedHIH6130()
        self.performTests(myHIH)

    def test_update(self):
        # Set the update time really low, to make sure that the values do
        # actually update. To check that an update actually occurs, need to
        # reach inside the class and put invalid values in the buffered data.
        myHIH = BufferedHIH6130(updateTime=0.05)
        oldData = myHIH.getData()
        myHIH._data = [-1,-1,1000] # Invalidate the buffer
        time.sleep(0.1) # Sleep long enough for data to be stale
        self.performTests(myHIH)
        data = myHIH.getData()
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0], 0)
        self.assertGreaterEqual(data[1], 0)
        self.assertLessEqual(data[1], 100)
        self.assertGreaterEqual(data[2], -25)
        self.assertLessEqual(data[2], 85)

    def performTests(self, device):
        # This should run fast enough that all of these calls return the same
        # buffered values.
        data1 = device.getData()
        data2 = device.getData()
        self.assertEqual( data1, data2 )
        humidity = device.humidity()
        temp = device.temperature()
        self.assertEqual( data1[1], humidity )
        self.assertEqual( data1[2], temp )

        # Test the relative humidity.
        self.assertGreaterEqual(humidity, 0)
        self.assertLessEqual(humidity, 100)
        # Test the temperature. These temperatures are taken from the documented
        # operating temperature, so if you're testing when the temperature is outside
        # this range the chip probably won't work anyway.
        self.assertGreaterEqual(temp, -25)
        self.assertLessEqual(temp, 85)

if __name__ == "__main__":
    unittest.main()
