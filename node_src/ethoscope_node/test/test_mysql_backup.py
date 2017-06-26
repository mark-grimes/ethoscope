from nose.tools import assert_equal
from ethoscope_node.utils.mysql_db_writer import MySQLdbCSVWriter
#from ethoscope_node.utils.mysql_backup import DBNotReadyError
import os
import logging
import traceback
import re

def TestMySQLDump():

    try:

        mirror = MySQLdbCSVWriter("/Users/phrfp/Software/anaconda2/envs/ethoscope/home/tmp",
                        remote_db_name="test_etho_db",
                        remote_host="localhost",
                        remote_pass="",
                        remote_user="root")


        mirror.update_roi_tables()


    except Exception as e:
        print e
        logging.error(traceback.format_exc(e))

def TestMySQLDump_Enumerate():

    try:

        mirror = MySQLdbCSVWriter("/Users/phrfp/Software/anaconda2/envs/ethoscope/home/tmp",
                        remote_db_name="test_etho_db",
                        remote_host="localhost",
                        remote_pass="",
                        remote_user="root")


        rowgen = mirror.enumerate_roi_tables()

        irow = 0

        for row in rowgen:
            if irow == 0:
                testrow = re.split(r'\t+', row.rstrip('\n'))
                print testrow
                assert_equal(testrow[0], "id", "Error: first column should be id")
                assert_equal(len(testrow), 11, "Error should have 11 columns in the current format")
                assert_equal(testrow[10], "roi", "Error: last column should be roi")
                irow = irow+1
            else:
                testrow = re.split(r'\t+', row.rstrip('\n'))
                print testrow
                assert_equal(testrow[1], "8000", "Error: first column should be 8000")
                assert_equal(len(testrow), 11, "Error should have 11 columns in the current format")
                assert_equal(testrow[7], "-280", "Error: 7th column should be -280")
                break
    except Exception as e:
        print e
        logging.error(traceback.format_exc(e))
