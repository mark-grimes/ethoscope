import logging
import time
try:
    import RPi.GPIO
except ImportError:
    # Assume we're running in a non-RPi environment (probably offline tracking) and
    # ignore. The class probably won't be used.
    logging.warning("Unable to import RPi.GPIO VibrationMotor will not work")
from ethoscope.hardware.interfaces.interfaces import BaseInterface


class VibrationMotorDevice():
    """
    Class to control a simple vibration motor (like in mobile phones) connected to a GPIO pin.
    This is used to stimulate the whole arena, rather than individual regions.
    """
    def __init__(self, gpio_pin=26):
        self._gpio_pin = gpio_pin
        RPi.GPIO.setmode(RPi.GPIO.BOARD)
        RPi.GPIO.setup(self._gpio_pin, RPi.GPIO.OUT, initial=RPi.GPIO.LOW)

    def vibrate_for(self,arrayOfSeconds):
        # If the input is a scalar convert to an array
        if not isinstance(arrayOfSeconds,list) : arrayOfSeconds = [arrayOfSeconds]

        try:
            for buzzTime in arrayOfSeconds:
                self.toggle_vibrating()
                time.sleep(buzzTime)
        finally:
            self.stop_vibrating()

    def start_vibrating(self):
        RPi.GPIO.output(self._gpio_pin,RPi.GPIO.HIGH)

    def stop_vibrating(self):
        RPi.GPIO.output(self._gpio_pin,RPi.GPIO.LOW)

    def toggle_vibrating(self):
        RPi.GPIO.output(self._gpio_pin,not RPi.GPIO.input(self._gpio_pin))

class VibrationMotorInterface(BaseInterface):
    def __init__(self, gpio_pin=26):
        self._hardware = VibrationMotorDevice(gpio_pin)
        self._last_time = None # The last time this was activated

    def _warm_up(self):
        pass

    def send(self, pulses_in_seconds=None, current_time=None):
        if pulses_in_seconds==None: pulses_in_seconds = [0.1,0.2,0.1,0.2,0.1,0.2,0.5] # arbitrary default
        
        # Need to make sure that this is not called from multiple places with the same
        # intended time. So ignore if "current_time" has been used before.
        if current_time!=None and current_time==self._last_time:
            return

        self._last_time = current_time
        self._hardware.vibrate_for( pulses_in_seconds )
