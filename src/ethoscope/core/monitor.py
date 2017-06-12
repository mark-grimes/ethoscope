__author__ = 'quentin'

from tracking_unit import TrackingUnit
import logging
import traceback
from multiprocessing.dummy import Pool  # Do the tracking in multiple threads (".dummy" uses the multiprocessing interface for multithreading)


class Monitor(object):

    def __init__(self, camera, tracker_class,
                 rois = None, stimulators=None,
                 *args, **kwargs  # extra arguments for the tracker objects
                 ):
        r"""
        Class to orchestrate the tracking of multiple objects.
        It performs, in order, the following actions:

         * Requesting raw frames (delegated to :class:`~ethoscope.hardware.input.cameras.BaseCamera`)
         * Cutting frame portions according to the ROI layout (delegated to :class:`~ethoscope.core.tracking_unit.TrackingUnit`).
         * Detecting animals and computing their positions and other variables (delegated to :class:`~ethoscope.trackers.trackers.BaseTracker`).
         * Using computed variables to interact physically (i.e. feed-back) with the animals (delegated to :class:`~ethoscope.stimulators.stimulators.BaseStimulator`).
         * Drawing results on a frame, optionally saving video (delegated to :class:`~ethoscope.drawers.drawers.BaseDrawer`).
         * Saving the result of tracking in a database (delegated to :class:`~ethoscope.utils.io.ResultWriter`).

        :param camera: a camera object responsible of acquiring frames and associated time stamps
        :type camera: :class:`~ethoscope.hardware.input.cameras.BaseCamera`
        :param tracker_class: The algorithm that will be used for tracking. It must inherit from :class:`~ethoscope.trackers.trackers.BaseTracker`
        :type tracker_class: class
        :param rois: A list of region of interest.
        :type rois: list(:class:`~ethoscope.core.roi.ROI`)
        :param stimulators: The class that will be used to analyse the position of the object and interact with the system/hardware.
        :type stimulators: list(:class:`~ethoscope.stimulators.stimulators.BaseInteractor`
        :param args: additional arguments passed to the tracking algorithm
        :param kwargs: additional keyword arguments passed to the tracking algorithm
        """

        self._camera = camera
        self._last_frame_idx =0
        self._force_stop = False
        self._last_positions = {}
        self._last_time_stamp = 0
        self._is_running = False
        self._process_pool = Pool(4) # Use 4 threads to do the tracking, since the RPi3 has 4 cores


        if rois is None:
            raise NotImplementedError("rois must exist (cannot be None)")

        if stimulators is None:
            self._unit_trackers = [TrackingUnit(tracker_class, r, None, *args, **kwargs) for r in rois]

        elif len(stimulators) == len(rois):
            self._unit_trackers = [TrackingUnit(tracker_class, r, inter, *args, **kwargs) for r, inter in zip(rois, stimulators)]
        else:
            raise ValueError("You should have one interactor per ROI")

    @property
    def last_positions(self):
        """
        :return: The last positions (and other recorded variables) of all detected animals
        :rtype: dict
        """
        return self._last_positions

    @property
    def last_time_stamp(self):
        """
        :return: The time, in seconds, since monitoring started running. It will be 0 if the monitor is not running yet.
        :rtype: float
        """
        time_from_start = self._last_time_stamp / 1e3
        return time_from_start

    @property
    def last_frame_idx(self):
        """
        :return: The number of the last acquired frame.
        :rtype: int
        """
        return self._last_frame_idx

    def stop(self):
        """
        Interrupts the `run` method. This is meant to be called by another thread to stop monitoring externally.
        """
        self._force_stop = True

    def run(self, result_writer = None, drawer = None):
        """
        Runs the monitor indefinitely.

        :param result_writer: A result writer used to control how data are saved. `None` means no results will be saved.
        :type result_writer: :class:`~ethoscope.utils.io.ResultWriter`
        :param drawer: A drawer to plot the data on frames, display frames and/or save videos. `None` means none of the aforementioned actions will performed.
        :type drawer: :class:`~ethoscope.drawers.drawers.BaseDrawer`
        """

        try:
            logging.info("Monitor starting a run")
            self._is_running = True

            for i,(t, frame) in enumerate(self._camera):

                if self._force_stop:
                    logging.info("Monitor object stopped from external request")
                    break

                self._last_frame_idx = i
                self._last_time_stamp = t
                self._frame_buffer = frame

                # Use the thread pool to do the tracking for better parallelism
                trackerFrame=TrackerFrame(t,frame)
                trackingResults=self._process_pool.map(trackerFrame.run,self._unit_trackers)

                for data_rows,roi,abs_pos in trackingResults:
                    if len(data_rows) == 0:
                        self._last_positions[roi.idx] = []
                        continue


                    # if abs_pos is not None:
                    self._last_positions[roi.idx] = abs_pos

                    if not result_writer is None:
                        result_writer.write(t,roi, data_rows)

                if result_writer is not None:
                    result_writer.flush(t, frame)

                if drawer is not None:
                    drawer.draw(frame, self._last_positions, self._unit_trackers)
                self._last_t = t

        except Exception as e:
            logging.error("Monitor closing with an exception: '%s'" % traceback.format_exc(e))
            raise e

        finally:
            self._is_running = False
            logging.info("Monitor closing")

class TrackerFrame():
    """
    Simple class to contain the "t" and "frame" values and make them
    available to the "run" function. Required since multiprocessing.Pool
    can (AFAIK) only take a function and the parameter array, not a list
    of parameters common to all processes (technically threads in our
    case since we're using multiprocessing.dummy).
    """
    def __init__(self,t,frame):
        self.t=t
        self.frame=frame
    def run(self,track_u):
        data_rows=track_u.track(self.t,self.frame)
        abs_pos=track_u.get_last_positions(absolute=True)
        return (data_rows,track_u.roi,abs_pos)


