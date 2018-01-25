import unittest
from ..mocks import Mock_MySQLdb
from ethoscope_node.utils.mysql_db_writer import MySQLdbCSVWriter
#from ethoscope_node.utils.mysql_backup import DBNotReadyError
import os
import logging
import traceback
import re


class TestMySQLCSVWriter(unittest.TestCase):
    def test_MySQLDumpCSV(self):

        root_dir = os.path.dirname(os.path.abspath(__file__)) # where all the test paths are relative to

        mirror = MySQLdbCSVWriter(
                        remote_db_name="test_etho_db",
                        remote_host="localhost",
                        remote_pass="",
                        remote_user="root")


        mirror.write_roi_tables(os.path.join(root_dir,os.pardir,"test_results"), True, 10, True)


    def test_MySQLDump_Enumerate(self):

        mirror = MySQLdbCSVWriter(
                        remote_db_name="test_etho_db",
                        remote_host="localhost",
                        remote_pass="",
                        remote_user="root")

        #
        # Inject the results expected
        #
        roiList = [[2], [3], [4]]
        Mock_MySQLdb.MockDBConnection.addMockResult(
                "SELECT roi_idx FROM ROI_MAP",
                roiList,
                None
            )
        description = [["id"], ["t"], ["x"], ["y"], ["w"], ["h"], ["phi"], ["xy_dist_log10x1000"], ["is_inferred"], ["has_interacted"]]
        for roiNumber in roiList:
            Mock_MySQLdb.MockDBConnection.addMockResult(
                    "SELECT * FROM ROI_%i" % roiNumber[0],
                    [
                        [1,8000,3,4,5,6,7,-280,9,10],
                        [2,8050,3,4,5,6,7,-280,9,10],
                        [3,8100,3,4,5,6,7,-280,9,10],
                    ],
                    description
                )

        rowgen = mirror.enumerate_roi_tables()

        irow = 0

        for row in rowgen:
            if irow == 0:
                testrow = re.split(r'\t+', row.rstrip('\n'))
                self.assertEquals(testrow[0], "id", "Error: first column should be id")
                self.assertEquals(len(testrow), 11, "Error should have 11 columns in the current format")
                self.assertEquals(testrow[10], "roi", "Error: last column should be roi")
                irow = irow+1
            else:
                testrow = re.split(r'\t+', row.rstrip('\n'))
                self.assertEquals(testrow[1], "8000", "Error: first column should be 8000")
                self.assertEquals(len(testrow), 11, "Error should have 11 columns in the current format")
                self.assertEquals(testrow[7], "-280", "Error: 7th column should be -280")
                self.assertEquals(testrow[10], "2", "Error: 10th column should be 2")
                irow = irow+1
                break

        self.assertEquals(irow,2) # Make sure the tests above were run

