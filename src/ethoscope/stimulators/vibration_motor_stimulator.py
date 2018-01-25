from ethoscope.stimulators.stimulators import BaseStimulator, HasInteractedVariable
from ethoscope.hardware.interfaces.vibration_motor import VibrationMotorInterface
import re, time, datetime

class VibrationMotorStimulator(BaseStimulator):
    """
    A stimulator that vibrates the whole arena at set times. The tracking information is ignored, the only
    check is if the time matches one of the preset vibration times.
    """
    _description = {"overview": "A stimulator to vibrate the whole arena",
                    "arguments": [
                                    {"type": "str",
                                     "name": "dates",
                                     "description": "A comma separated list of times to vibrate at, in the form 'YYYY-MM-DD HH:MM:SS', e.g. '2017-09-15 17:30:00, 2017-09-15 21:00:00'",
                                     "default": ""}
                                   ]}

    _HardwareInterfaceClass = VibrationMotorInterface

    def __init__(self, hardware_connection=None, dates = "", **kwargs):
        self._vibration_time = [0.4,0.2,0.4,0.2,0.4]

        self._times = []
        for date in dates.split(","):
            numericValue = self._parse_date(date)
            if numericValue!=None: self._times.append(numericValue)
        self._times.sort(reverse=True)

        super(VibrationMotorStimulator, self).__init__(hardware_connection, "")

    def _decide(self):
        if len(self._times)>0:
            if time.time()>self._times[-1]:
                current = self._times.pop()
                return HasInteractedVariable(True), {"pulses_in_seconds":self._vibration_time,"current_time":current}
        return HasInteractedVariable(False), {}        

    def _parse_date(self, str): # Copied from ethoscope.utils.scheduler.Scheduler
        pattern = re.compile("^\s*(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\s*$")
        if re.match("^\s*$", str):
            return None
        if not re.match(pattern, str):
            raise DateRangeError("%s not match the expected pattern" % str)
        datestr = re.match(pattern, str).groupdict()["date"]
        return time.mktime(datetime.datetime.strptime(datestr,'%Y-%m-%d %H:%M:%S').timetuple())
