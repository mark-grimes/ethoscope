#! /usr/bin/env python

"""
Tests for the TSL2591 class which talks to the TSL2591 light sensor.
"""

__author__    = "Mark Grimes"
__copyright__ = "Copyright 2017, Rymapt Ltd"
__license__   = "To be decided"

import unittest
import ethoscope.hardware.input.TSL2591 as TSL2591

class TestTSL2591(unittest.TestCase):

    def test_getRawValues(self):
        """
        Make sure we can get the raw values, and that higher gains provide higher results.
        """
        device = TSL2591.TSL2591()
        device.powerOn()
        device.waitForIntegration() # add an extra wait here, in case the integration time was changed

        results = {}
        for gain in [TSL2591.GAIN_LOW, TSL2591.GAIN_MED, TSL2591.GAIN_HIGH, TSL2591.GAIN_MAX]:
            device.setGain( gain )
            # need to perform a sleep because it takes a little bit of time to transition
            device.waitForIntegration()
            results[gain] = device.rawValues()

        # Channel 0 (visible and IR)
        self.assertTrue( results[TSL2591.GAIN_LOW][0] <= results[TSL2591.GAIN_MED][0], str(results[TSL2591.GAIN_LOW][0])+" < "+str(results[TSL2591.GAIN_MED][0]) )
        self.assertTrue( results[TSL2591.GAIN_MED][0] <= results[TSL2591.GAIN_HIGH][0], str(results[TSL2591.GAIN_MED][0])+" < "+str(results[TSL2591.GAIN_HIGH][0]) )
        self.assertTrue( results[TSL2591.GAIN_HIGH][0] <= results[TSL2591.GAIN_MAX][0], str(results[TSL2591.GAIN_HIGH][0])+" < "+str(results[TSL2591.GAIN_MAX][0]) )
        self.assertNotEqual( 0, results[TSL2591.GAIN_MAX][0] )

        # Channel 1 (IR only)
        self.assertTrue( results[TSL2591.GAIN_LOW][1] <= results[TSL2591.GAIN_MED][1], str(results[TSL2591.GAIN_LOW][1])+" < "+str(results[TSL2591.GAIN_MED][1]) )
        self.assertTrue( results[TSL2591.GAIN_MED][1] <= results[TSL2591.GAIN_HIGH][1], str(results[TSL2591.GAIN_MED][1])+" < "+str(results[TSL2591.GAIN_HIGH][1]) )
        self.assertTrue( results[TSL2591.GAIN_HIGH][1] <= results[TSL2591.GAIN_MAX][1], str(results[TSL2591.GAIN_HIGH][1])+" < "+str(results[TSL2591.GAIN_MAX][1]) )
        self.assertNotEqual( 0, results[TSL2591.GAIN_MAX][0] )

        device.powerOff()

    def test_getAndSetGain(self):
        device = TSL2591.TSL2591()

        device.setGain( TSL2591.GAIN_LOW )
        self.assertEqual( TSL2591.GAIN_LOW, device.getGain() );
        self.assertEqual( TSL2591.GAIN_LOW, 0x30 & device._readConfigRegister() );

        device.setGain( TSL2591.GAIN_MED )
        self.assertEqual( TSL2591.GAIN_MED, device.getGain() );
        self.assertEqual( TSL2591.GAIN_MED, 0x30 & device._readConfigRegister() );

        device.setGain( TSL2591.GAIN_HIGH )
        self.assertEqual( TSL2591.GAIN_HIGH, device.getGain() );
        self.assertEqual( TSL2591.GAIN_HIGH, 0x30 & device._readConfigRegister() );

        device.setGain( TSL2591.GAIN_MAX )
        self.assertEqual( TSL2591.GAIN_MAX, device.getGain() );
        self.assertEqual( TSL2591.GAIN_MAX, 0x30 & device._readConfigRegister() );

    def test_getAndSetIntegrationTime(self):
        device = TSL2591.TSL2591()

        device.setIntegrationTime( TSL2591.INTEGRATIONTIME_100MS )
        self.assertEqual( TSL2591.INTEGRATIONTIME_100MS, device.getIntegrationTime() );
        self.assertEqual( 0.1, device.getIntegrationTimeSeconds() );
        self.assertEqual( TSL2591.INTEGRATIONTIME_100MS, 0x07 & device._readConfigRegister() );

        device.setIntegrationTime( TSL2591.INTEGRATIONTIME_200MS )
        self.assertEqual( TSL2591.INTEGRATIONTIME_200MS, device.getIntegrationTime() );
        self.assertEqual( 0.2, device.getIntegrationTimeSeconds() );
        self.assertEqual( TSL2591.INTEGRATIONTIME_200MS, 0x07 & device._readConfigRegister() );

        device.setIntegrationTime( TSL2591.INTEGRATIONTIME_300MS )
        self.assertEqual( TSL2591.INTEGRATIONTIME_300MS, device.getIntegrationTime() );
        self.assertEqual( 0.3, device.getIntegrationTimeSeconds() );
        self.assertEqual( TSL2591.INTEGRATIONTIME_300MS, 0x07 & device._readConfigRegister() );

        device.setIntegrationTime( TSL2591.INTEGRATIONTIME_400MS )
        self.assertEqual( TSL2591.INTEGRATIONTIME_400MS, device.getIntegrationTime() );
        self.assertEqual( 0.4, device.getIntegrationTimeSeconds() );
        self.assertEqual( TSL2591.INTEGRATIONTIME_400MS, 0x07 & device._readConfigRegister() );

        device.setIntegrationTime( TSL2591.INTEGRATIONTIME_500MS )
        self.assertEqual( TSL2591.INTEGRATIONTIME_500MS, device.getIntegrationTime() );
        self.assertEqual( 0.5, device.getIntegrationTimeSeconds() );
        self.assertEqual( TSL2591.INTEGRATIONTIME_500MS, 0x07 & device._readConfigRegister() );

        device.setIntegrationTime( TSL2591.INTEGRATIONTIME_600MS )
        self.assertEqual( TSL2591.INTEGRATIONTIME_600MS, device.getIntegrationTime() );
        self.assertEqual( 0.6, device.getIntegrationTimeSeconds() );
        self.assertEqual( TSL2591.INTEGRATIONTIME_600MS, 0x07 & device._readConfigRegister() );

    def test_getLux(self):
        device = TSL2591.TSL2591()
        device.powerOn()

        results = {}
        for gain in [TSL2591.GAIN_LOW, TSL2591.GAIN_MED]:
            device.setGain( gain )
            # need to perform a sleep because it takes a little bit of time to transition
            device.waitForIntegration()
            results[gain] = device.lux()

        self.assertTrue( results[TSL2591.GAIN_LOW] > 0 )
        self.assertTrue( results[TSL2591.GAIN_MED] > 0 )

        # Assume indoors, with non-direct light
        self.assertTrue( results[TSL2591.GAIN_LOW] < 100 )
        self.assertTrue( results[TSL2591.GAIN_MED] < 100 )

        device.powerOff()

if __name__ == "__main__":
    unittest.main()
