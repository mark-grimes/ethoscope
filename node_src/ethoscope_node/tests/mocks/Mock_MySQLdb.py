"""
Provides a mock database connection by intercepting calls to MySQLdb.connect() and returning
a fake class.

So far not very exhaustive mocking, but this will be expanded as the test cases are expanded.
"""

import collections

class MockDBConnection(object):
    execute_results = {}

    #
    # Methods to set up the mocking behaviour
    #
    @staticmethod
    def addMockResult(command, results, description):
        """
        Static method to set the results that a call to "execute" should give.
        """
        MockDBConnection.execute_results[command] = {"results": collections.deque(results), "description": description}

    #
    # Methods for the class returned by MySQLdb.connect
    #
    def __init__(self,**kwargs):
        super(MockDBConnection,self).__init__()
        self._kwargs = kwargs
        self._mock_results = []
        self.description = []
    
    def cursor(self):
        return self

    def close(self):
        pass

    #
    # Methods from the cursor class
    #
    def __iter__(self):
        return self

    def next(self):
        if len(self._mock_results) == 0:
            raise StopIteration
        return self._mock_results.popleft()

    def execute(self,command):
        self._mock_results = []
        self.description = []

        try:
            mockResults = MockDBConnection.execute_results[command]
        except KeyError:
            return

        self._mock_results = mockResults["results"]
        self.description = mockResults["description"]



def connect(**kwargs):
    return MockDBConnection(**kwargs)

# Overwrite the connect function in MySQLdb so that the mock classes are
# used instead.
import MySQLdb
MySQLdb.original_connect = MySQLdb.connect # make a note so that it can be restored if necessary
MySQLdb.overriden_connect = connect
MySQLdb.connect = MySQLdb.overriden_connect # Default to overriding
