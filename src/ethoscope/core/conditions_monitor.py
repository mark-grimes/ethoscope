__author__ = "grimes"

import multiprocessing
import logging
import time
import sqlalchemy

class ConditionVariable(object):
    """
    A convenience class for code to make variables available to the ConditionMonitor. Code would create an
    instance of ConditionVariable by passing it a multiprocessing.Value and name for the variable, and passing
    the new ConditionVariable instance to ConditionsMonitor. When the variable changes they should modify
    their copy of the multiprocessing.Value directly. I.e. the code monitoring the condition would keep a
    reference to the multiprocessing.Value and forget about the ConditionVariable once it was passed to
    ConditionsMonitor.

    You don't need to use this class, as long as ConditionsMonitor receives an object with "value()" and
    "name()" methods, and that the "value()" method is multiprocessing safe.
    """
    def __init__(self, value, name, description = None):
        self._variable = value
        self._name = name
        self._description = description

        # required for the database
        self.header_name = name
        self.sql_data_type = "SMALLINT"
        self.functional_type = "not set yet"
    def value(self):
        return self._variable.value
    def name(self):
        return self._name
    def description(self):
        return self._description

class ConditionVariableFunction(object):
    """
    Similar to ConditionVariable except that it takes a function to generate the value.
    """
    def __init__(self, function, name, description = None):
        self._function = function
        self._name = name
        self._description = description
    def value(self):
        return self._function()
    def name(self):
        return self._name
    def description(self):
        return self._description

class ConditionsProcess(multiprocessing.Process):
    """
    The portion of ConditionsMonitor that runs in a different process. This should never be invoked
    directly, only through ConditionsMonitor.
    """
    def __init__(self, keepRunning, updatePeriodSeconds, pollingCondition, conditionVariables):
        super(ConditionsProcess,self).__init__()
        self._keepRunning = keepRunning
        self._updatePeriodSeconds = updatePeriodSeconds
        self._pollingCondition = pollingCondition
        self._sqlalchemy_engine = None
        self._tableName = "CONDITIONS"
        self._conditionVariables = conditionVariables
        self._isRunning = multiprocessing.Value('b', False)
        self._timeOffset = multiprocessing.Value('d', 0) # Needs to be double, I get numerical errors with a single float
        self._timeCoefficient = multiprocessing.Value('f', 1000) # default to using time in milliseconds

    def set_writer(self,sqlalchemy_engine):
        if self._isRunning.value:
            raise Exception("Unable to change writer while running")

        self._sqlalchemy_engine = sqlalchemy_engine

    def run(self):
        logging.info("ConditionsMonitor starting")
        self._isRunning.value = True

        try:
            # Make sure the database has the table in place
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(self._tableName,
                                     metadata,
                                     sqlalchemy.Column('id', sqlalchemy.Integer, sqlalchemy.Sequence('entry_id_seq'), primary_key=True),
                                     sqlalchemy.Column('t', sqlalchemy.Integer ) )
            for variable in self._conditionVariables:
                # Get one value to see what type it is
                valueType = sqlalchemy.Integer
                try:
                    value = variable.value()
                    if type(value) is float: valueType = sqlalchemy.Float
                    elif type(value) is int: valueType = sqlalchemy.Integer
                    else:
                        logging.warning("Unable to determine database type for '"+str(type(value))+"', assuming integer")
                except Exception as error:
                    logging.error("Unable to determine database type for '"+variable.name()+"' "+str(error))
                table.append_column( sqlalchemy.Column( variable.name(), valueType) )
            metadata.create_all(self._sqlalchemy_engine)

            while self._keepRunning.value:
                results = {"t": int( (time.time() - self._timeOffset.value)*self._timeCoefficient.value )}
                for variable in self._conditionVariables:
                    try:
                        name = variable.name()
                        value = variable.value()
                        results[name] = value
                    except Exception as error:
                        try:
                            name = variable.name()
                        except:
                            name = "<exception getting name>"
                        logging.error("Condition variable '"+name+"' could not be logged because it produced the exception: "+str(error))

                connection = self._sqlalchemy_engine.connect()
                connection.execute(table.insert(), [results] )

                with self._pollingCondition:
                    self._pollingCondition.wait(self._updatePeriodSeconds.value)
        finally:
            logging.info("ConditionsMonitor stopping")
            self._isRunning.value = False

class ConditionsMonitor(object):
    """
    Class that monitors condition variables (e.g. light, temperature etcetera) and writes them to the
    database.

    Underneath it starts another process to do the work, but all interaction should be through this class.
    """
    def __init__(self, conditionVariables):
        self._keepRunning = multiprocessing.Value('b',True)
        self._updatePeriodSeconds = multiprocessing.Value('f',30)
        self._pollingCondition = multiprocessing.Condition()
        self._process = ConditionsProcess(self._keepRunning, self._updatePeriodSeconds, self._pollingCondition, conditionVariables)

    def run(self, connection_address=None, sqlalchemy_engine = None):
        """
        Start the monitor running asynchronously in another process. Use 'stop()' to stop it. You need to provide a place
        to send the results by providing an SQL database location, e.g.

            "mysql://username:password@localhost/database_name"
            "sqlite:///myfile.sqlite3"

        The strings are in the format required by sqlalchemy. You can also instead provide an sqlalchemy engine directly with the
        sqlalchemy_engine keyword parameter. For a full list of possible strings, and how to configure your own engine, see

            http://docs.sqlalchemy.org/en/latest/core/engines.html

        """
        if sqlalchemy_engine:
            if connection_address!= None : logging.warning("ConditionsMonitor: An SQLAlchemy engine was provided, so the value of connection_address is being ignored")
            self._process.set_writer(sqlalchemy_engine)
        elif connection_address:
            self._process.set_writer( sqlalchemy.create_engine(connection_address) )
        else:
            raise Exception("ConditionsMonitor.run() called without a connection address or SQLAlchemy engine")
        self._process.start()

    def stop(self):
        self._keepRunning.value = False
        with self._pollingCondition:
            self._pollingCondition.notify() # Wake rather than wait for the full polling time
        self._process.join()

    def updatePeriod(self):
        """
        Get the time in seconds that the monitor takes between updates.
        """
        return self._updatePeriodSeconds.value

    def updatePeriod(self,time):
        """
        Set the time in seconds that the monitor takes between updates.

        When this is called, the monitor immediately updates and then uses the new period after
        that.
        """
        self._updatePeriodSeconds.value = time
        # I'll wake the process so that it immediately starts using the new period rather than
        # finish the current wait. This has the side effect of having an immediate update, but
        # rather that than having it wait for the full old period in case an exceptionally long
        # time was previously set.
        with self._pollingCondition:
            self._pollingCondition.notify()

    def setTime(self, referenceTime, coefficient=1000):
        """
        Offset the times recorded in the database by specifying what the time should be right now.
        Also allow different units by passing a value other than 1 for 'coefficient'. 'coefficient'
        will be applied to the time in seconds to get the value that is put in the database.
        E.g. a coefficient of 1000 will mean the time in the database will be milliseconds. A
        coefficient of 1 will mean the time in the database is in seconds. Default is milliseconds.
        """
        self._process._timeOffset.value = time.time() - float(referenceTime)/float(coefficient)
        self._process._timeCoefficient.value = coefficient
        
    def tableName(self):
        """
        Get the name of the database table results are written to
        """
        return self._process._tableName
