#! /usr/bin/env python

"""
Tests for the MySQLdbConverter class.
"""
__author__    = "Mark Grimes"
__copyright__ = "Copyright 2017, Rymapt Ltd"
__license__   = "MIT"
# MIT licence is available at https://opensource.org/licenses/MIT
# Note that the ethoscope software is GPL 3, so when included in that source tree this file becomes GPL 3. By
# being licensed as MIT enables this file (and this alone) to be used in other projects under the MIT licence.

import unittest
from ethoscope_node.utils.mysql_db_converter import MySQLdbConverter
import os
import logging


class TestMySQLCSVWriter(unittest.TestCase):
    def test_MySQLConvertToSQLite(self):
        """
        This is a rubbish test. So far all I'm testing is that it doesn't crash when trying to access the ethoscope
        device's database. It doesn't even set that up, it needs the database to be already running.
        
        TODO - Write some decent tests with well known inputs. Probably have an SQLite file in the repository as test
        data.
        """

        root_dir = os.path.dirname(os.path.abspath(__file__)) # where all the test paths are relative to

        converter = MySQLdbConverter()
        converter.copy_database("sqlite:////"+root_dir+"/testDump.sqlite3")

if __name__ == "__main__":
    unittest.main()
