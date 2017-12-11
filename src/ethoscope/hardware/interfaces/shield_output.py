"""
Code to control anything connected to the outputs on the Rymapt shield
"""
import logging
import smbus
import RPi.GPIO
from ethoscope.hardware.interfaces.interfaces import BaseInterface


class ShieldOutput():
    """
    Class to control anything attached to one of the output connections on the Rymapt shield. You
    can specify which of the shield connections it's on with the 'shield_pin' parameter, with 0
    being the leftmost connection (as you look at the shield with the connections towards you) and
    4 being the rightmost connection.
    Alternatively you can specify the GPIO pin by hand with the 'gpio_pin' parameter, in which case
    'shield_pin' is ignored.
    """
    shield_pins = [24,26,31,29,7] # The GPIO numbers for the shield pins

    def __init__(self, shield_pin=None, gpio_pin=None, check_shield=True):
        if shield_pin==None and gpio_pin==None: raise Exception("Either shield_pin or gpio_pin must be specified")
        if check_shield:
            try:
                # See if the TSL2591 light sensor chip is attached on I2C address 0x29. It has a hard coded ID at register 0x12. 
                bus=smbus.SMBus(1)
                bus.write_byte( 0x29, (0xa0 | 0x12) ) # Need to write the address we want from the read
                if bus.read_byte(0x29) != 0x50: # Chip should have a hard code ID of 0x50
                    raise Exception()
            except:
                self._shield_attached = False
                logging.warning("Unable to determine that the Rymapt shield is connected")
                return
            self._shield_attached = True
        else:
            self._shield_attached = True # Check was skipped, so just assume shield is attached

        if gpio_pin==None:
            self._gpio_pin = ShieldOutput.shield_pins[shield_pin]
        else:
            self._gpio_pin = gpio_pin
        RPi.GPIO.setmode(RPi.GPIO.BOARD)
        RPi.GPIO.setup(self._gpio_pin, RPi.GPIO.OUT, initial=RPi.GPIO.LOW)

    def on(self):
        if self._shield_attached:
            RPi.GPIO.output(self._gpio_pin,RPi.GPIO.HIGH)
        else:
            logging.warning("Rymapt shield is not attached so cannot switch on output")

    def off(self):
        if self._shield_attached:
            RPi.GPIO.output(self._gpio_pin,RPi.GPIO.LOW)
        else:
            logging.warning("Rymapt shield is not attached so cannot switch off output")
