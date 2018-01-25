#! /usr/bin/env python

"""
Tests for the ConditionsMonitor class
"""
__author__    = "Mark Grimes"
__copyright__ = "Copyright 2017, Rymapt Ltd"
__licence__   = "MIT"
# MIT licence is available at https://opensource.org/licenses/MIT

import unittest
from ethoscope.core.conditions_monitor import ConditionsMonitor
import sqlalchemy
import time
import random
import os

class DummyTestingConditionVariable(object):
    """
    Creates an array of random numbers and then returns each one in sequence for
    each call of 'value()'. When the sequence is exhausted, it wraps around and
    start returning the **same** numbers. You can repeat the sequence by calling
    'reset()' and then 'value()' the required number of times.
    """
    def __init__(self, name="RandomNumber", size=50):
        self._name = name
        self._values = [0]*size
        for index in xrange(0,size):
            self._values[index] = random.randint(0,100)
        self._nextIndex = 0
    def value(self):
        returnValue = self._values[self._nextIndex]
        self._nextIndex = (self._nextIndex+1)%len(self._values)
        return returnValue
    def name(self):
        return self._name
    def reset(self):
        self._nextIndex = 0

class DummyFloatConditionVariable(DummyTestingConditionVariable):
    """
    Same as DummyTestingConditionVariable except the returned value is a float
    """
    def __init__(self, name="RandomNumber", size=50):
        super(DummyFloatConditionVariable,self).__init__(name,size)
        # Run through and change the numbers to floats by dividing by 100
        for index in xrange(0,len(self._values)):
            self._values[index] = float(self._values[index])/100.0

class TestConditionsMonitor(unittest.TestCase):
    def test_insertingRandomNumbers(self):
        # In memory SQLite databases won't work because they're released before I can check
        # the results. Use a random portion to the filename to minimise chances of a clash
        temporaryFilename = "tmp_test_db_"+str(random.randint(0,1000))+".sqlite"
        try:
            variables = [DummyTestingConditionVariable("Field1"), DummyTestingConditionVariable("Field2"), DummyFloatConditionVariable("FloatField")]
            monitor = ConditionsMonitor(variables)
            engine = sqlalchemy.create_engine('sqlite:///'+temporaryFilename)
            #engine = sqlalchemy.create_engine('mysql://ethoscope:ethoscope@localhost/ethoscope_db', echo=True)
            monitor.updatePeriod(0.01) # Update every hundredth of a second to speed up the tests
            startTime = time.time()
            monitor.setTime(0) # Test setting a time offset by specifying 'now' as 0 time
            monitor.run(sqlalchemy_engine=engine)
            time.sleep(1)
            monitor.stop()
            stopTime = time.time()
            
            # Now check the results that were inserted
            metadata = sqlalchemy.MetaData(engine)
            table = sqlalchemy.Table( monitor.tableName(), metadata, autoload=True )
            # Make sure the columns are the correct type
            columnsChecked = 0
            for column in table.columns:
                if column.name=="Field1":
                    self.assertEqual( "INTEGER", str(column.type) )
                    columnsChecked += 1
                elif column.name=="Field2":
                    self.assertEqual( "INTEGER", str(column.type) )
                    columnsChecked += 1
                elif column.name=="FloatField":
                    self.assertEqual( "FLOAT", str(column.type) )
                    columnsChecked += 1
            self.assertEqual( 3, columnsChecked )
            # Query the database for the values
            select = sqlalchemy.select([table])
            connection = engine.connect()
            results = connection.execute( select )
            for variable in variables:
                variable.reset()
                variable.value() # Advance by 1 because internally ConditionsMonitor retrieves 1 value to get its type
            index = 0
            for index, row in enumerate(results):
                self.assertEqual(row[0], index+1) # Check index increases by 1 each time
                self.assertGreaterEqual(row[1], 0) # Check time is greater than the reference time I set (zero)
                self.assertLessEqual(row[1], (stopTime-startTime)*1000) # default is to store time in milliseconds
                for variableIndex, variable in enumerate(variables):
                    self.assertEqual(row[variableIndex+2], variable.value())
            self.assertGreaterEqual(index, 10) # Make sure at least some records were created
        finally:
            try:
                os.remove(temporaryFilename)
            except OSError:
                pass # probably was never created, let the original exception show through

if __name__ == "__main__":
    unittest.main()
