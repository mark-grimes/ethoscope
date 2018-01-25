#! /usr/bin/env python

"""
Script that will download a remote database and save it as SQLite.
"""

from ethoscope_node.utils.mysql_db_converter import MySQLdbConverter
import optparse

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-u", "--username", dest="username", default="ethoscope",help="The username for the remote database")
    parser.add_option("-p", "--password", dest="password", default="ethoscope",help="The password for the remote database")
    parser.add_option("--host", dest="host", default="localhost", help="The hostname that the remote database is running on")
    parser.add_option("--port", dest="port", default="3306", help="The port that the remote database is running on")
    parser.add_option("-d", "--database", dest="database", default="ethoscope_db", help="The database name on the remote computer")
    parser.add_option("-o", "--output", dest="output", default="ethoscope_db.sqlite", help="The output filename on the local computer")

    (options, args) = parser.parse_args()
    
    skip_tables=["IMG_SNAPSHOTS"]
    converter = MySQLdbConverter( input_address="mysql://"+options.username+":"+options.password+"@"+options.host+":"+options.port+"/"+options.database )
    converter.copy_database("sqlite:///"+options.output, skip_tables=skip_tables)
