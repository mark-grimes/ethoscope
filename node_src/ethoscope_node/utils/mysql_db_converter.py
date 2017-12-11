"""
Class for using SQLAlchemy (http://www.sqlalchemy.org/) for converting from the MySQL database
to different database types.
"""
__author__    = "Mark Grimes"
__copyright__ = "Copyright 2017, Rymapt Ltd"
__license__   = "MIT"
# MIT licence is available at https://opensource.org/licenses/MIT
# Note that the ethoscope software is GPL 3, so when included in that source tree this file becomes GPL 3. By
# being licensed as MIT enables this file (and this alone) to be used in other projects under the MIT licence.

import logging
import sqlalchemy
import sqlalchemy.ext.compiler
import sqlalchemy.dialects.mysql

# Define the custom mappings from MySQL to SQLite that aren't covered by the defaults
@sqlalchemy.ext.compiler.compiles(sqlalchemy.dialects.mysql.TINYINT, 'sqlite')
def compile_TINYINT(element, compiler, **kw):
    """Treat MySQL TINYINT as normal integer in SQLite"""
    return compiler.visit_integer(element, **kw)

@sqlalchemy.ext.compiler.compiles(sqlalchemy.dialects.mysql.LONGBLOB, 'sqlite')
def compile_LONGBLOB(element, compiler, **kw):
    """Treat MySQL LONGBLOB as large binary in SQLite"""
    return compiler.visit_large_binary(element, **kw)

class MySQLdbConverter(object):
    """
    This class can be used to convert the MySQL database to any format supported by SQLAlchemy. It's used for the SQLite
    download functionality. Note that the class MySQLdbToSQlite in mysql_backup.py does something similar except that it
    is selective about which data it converts to the SQLite file. This class just copies all data verbatim which has the
    advantage of including conditions data and anything added by later development, but the down side of including lots
    of data.
    """

    def __init__(self,
                            remote_db_name="ethoscope_db",
                            remote_host="localhost",
                            remote_user="ethoscope",
                            remote_pass="ethoscope",
                            input_engine=None,
                            input_address=None
                            ):
        """
        :param remote_db_name: the name of the database running locally.
        :param remote_host: the ip of the database - localhost will be fully test remote testing to follow
        :param remote_user: the user name for the database
        :param remote_pass: the password for the database
        :param input_engine: an SQLAlchemy engine for the input database. If supplied then remote_db_name,
                             remote_host, remote_user and remote_pass are silently ignored. If input_address
                             is specified that is ignored (i.e. input_engine takes precedence).
        :param input_address: an SQLAlchemy location of the input database. If supplied then remote_db_name,
                              remote_host, remote_user and remote_pass are silently ignored.
        """

        if input_engine:
            if input_address!= None : logging.warning("MySQLdbConverter: An SQLAlchemy engine was provided, so the value of input_address is being ignored")
            self._input_engine = input_engine
        elif input_address:
            self._input_engine = sqlalchemy.create_engine(input_address)
        else:
            self._input_engine = sqlalchemy.create_engine("mysql://"+remote_user+":"+remote_pass+"@"+remote_host+"/"+remote_db_name)

        self._batchSize = 200 # The number of inserts to group so that copying is faster
        self._lastID = {}

    def copy_database(self, connection_address=None, sqlalchemy_engine = None, skip_tables = None):
        """
        Copy the database to a new database at the location provided. The strings are in the format required by sqlalchemy e.g.

            "sqlite:///myfile.sqlite3" - SQLite file at the location ./myfile.sqlite3 (note 3 backslashes)
            "sqlite:////tmp/myfile.sqlite3" - SQLite file at the location /tmp/myfile.sqlite3 (note 4 backslashes)

        You can also instead provide an sqlalchemy engine directly with the sqlalchemy_engine keyword parameter. For a full list
        of possible strings, and how to configure your own engine, see

            http://docs.sqlalchemy.org/en/latest/core/engines.html

        This function is highly likely to fail if the file already exists.

        You can also provide a list of table names that will not be included in the copy with the "skip_tables" parameter. E.g.
        'skip_tables=["IMG_SNAPSHOTS"]' will not include image snapshots in the copied database and will save a considerable
        amount of space.
        """
        if sqlalchemy_engine:
            if connection_address!= None : logging.warning("MySQLdbConverter: An SQLAlchemy engine was provided, so the value of connection_address is being ignored")
            self._sqlalchemy_engine = sqlalchemy_engine
        elif connection_address:
            self._sqlalchemy_engine = sqlalchemy.create_engine(connection_address)
        else:
            raise Exception("MySQLdbConverter.copy_database() called without a connection address or SQLAlchemy engine")

        if skip_tables==None:
            skip_tables=[]

        inputMetadata = sqlalchemy.MetaData(bind=self._input_engine)
        inputMetadata.reflect(self._input_engine)
        outputMetadata = sqlalchemy.MetaData(bind=self._sqlalchemy_engine)

        for tableName, inputTable in inputMetadata.tables.iteritems():
            #
            # First copy the schema for the table
            #


            outputTable = sqlalchemy.Table(inputTable.name, outputMetadata)
            for inputColumn in inputTable.columns:
                outputTable.append_column(inputColumn.copy())
            outputTable.create()
            #
            # Then copy the data
            #
            insert = outputTable.insert()
            # Group the inserts into the destination into batches to speed up the copy.
            bulkRows = []
            lastID = None
            for row in inputTable.select().execute():
                bulkRows.append(row)
                if tableName == "CONDITIONS":
                    lastID = row['id']
                if tableName.startswith("ROI_"):
                    try: # See if the rest is a number (i.e. not "ROI_MAP")
                        roi = int(tableName[4:])
                        lastID = row['id']
                    except ValueError: continue

                if len(bulkRows) > self._batchSize :
                    insert.execute(bulkRows)
                    bulkRows = []
            if len(bulkRows) > 0 :
                insert.execute(bulkRows)
            if lastID != None:
                self._lastID[tableName] = lastID

    def update_database(self):
        """
        Get the last insert and add from there
        """
        inputMetadata = sqlalchemy.MetaData(bind=self._input_engine)
        inputMetadata.reflect(self._input_engine)
        outputMetadata = sqlalchemy.MetaData(bind=self._sqlalchemy_engine)
        tablesToUpdate = []
        for tableName in self._sqlalchemy_engine.table_names():
            if tableName == "CONDITIONS": tablesToUpdate.append(tableName)
            if tableName.startswith("ROI_"):
                try: # See if the rest is a number (i.e. not "ROI_MAP")
                    roi = int(tableName[4:])
                    tablesToUpdate.append(tableName)
                except ValueError: continue

        # Queries are done in batches so that the results are available as they are created. This
        # can take a little longer overall, but if running in multiple processes allows getting
        # some results before the whole thing has finished.
        while len(tablesToUpdate) > 0:
            tablesToRemove = [] # Tables that no longer need updating (don't want to remove from tablesToUpdate during the loop)

            for tableName in tablesToUpdate:
                logging.info("DB BACK UP: Adding table "+tableName)

                inputtable = sqlalchemy.Table( tableName, inputMetadata, autoload=True )
                outputTable = sqlalchemy.Table( tableName, outputMetadata)
                try:
                    startID = self._lastID[tableName]
                except KeyError:
                    startID = 0

                if startID != 0:
                    select = sqlalchemy.select([inputtable]).where(inputtable.c.id > startID)
                else:
                    select = sqlalchemy.select([inputtable])

                connection = self._input_engine.connect()
                results = connection.execute( select )
                insert = outputTable.insert()
                # Group the inserts into the destination into batches to speed up the copy.
                bulkRows = []
                lastID = 0
                for row in results:
                    bulkRows.append(row)
                    lastID = row['id']
                    if len(bulkRows) > self._batchSize :
                        insert.execute(bulkRows)
                        bulkRows = []
                if len(bulkRows) > 0 :
                    insert.execute(bulkRows)
                self._lastID[tableName] = lastID

                tablesToRemove.append(tableName)
            # Remove all the tables that are fully up to date before the next loop
            for tableName in tablesToRemove:
                tablesToUpdate.remove(tableName)
