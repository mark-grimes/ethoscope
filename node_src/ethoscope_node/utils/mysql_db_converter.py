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

        self._batchSize = 2000 # The number of inserts to group so that copying is faster

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
        elif connection_address:
            sqlalchemy_engine = sqlalchemy.create_engine(connection_address)
        else:
            raise Exception("MySQLdbConverter.copy_database() called without a connection address or SQLAlchemy engine")

        if skip_tables==None:
            skip_tables=[]

        inputMetadata = sqlalchemy.MetaData(bind=self._input_engine)
        inputMetadata.reflect(self._input_engine)
        outputMetadata = sqlalchemy.MetaData(bind=sqlalchemy_engine)

        for tableName, inputTable in inputMetadata.tables.iteritems():
            #
            # First copy the schema for the table
            #
            if tableName in skip_tables : continue

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
            for row in inputTable.select().execute():
                bulkRows.append(row)
                if len(bulkRows) > self._batchSize :
                    insert.execute(bulkRows)
                    bulkRows = []
            if len(bulkRows) > 0 :
                insert.execute(bulkRows)

    def update_database(self):
        """
        Get the last insert and add from there
        """

        if sqlalchemy_engine:
            if connection_address!= None : logging.warning("MySQLdbConverter: An SQLAlchemy engine was provided, so the value of connection_address is being ignored")
        elif connection_address:
            sqlalchemy_engine = sqlalchemy.create_engine(connection_address)
        else:
            raise Exception("MySQLdbConverter.copy_database() called without a connection address or SQLAlchemy engine")

        if skip_tables==None:
            skip_tables=[]

        inputMetadata = sqlalchemy.MetaData(bind=self._input_engine)
        inputMetadata.reflect(self._input_engine)
        outputMetadata = sqlalchemy.MetaData(bind=sqlalchemy_engine)

        for tableName, inputTable in inputMetadata.tables.iteritems():
            #
            # First copy the schema for the table
            #
            if tableName in skip_tables : continue

            outputTable = sqlalchemy.Table(inputTable.name, outputMetadata)
            #
            # Then copy the data
            #
            update = outputTable.update()
            # Group the inserts into the destination into batches to speed up the copy.
            bulkRows = []
            for row in inputTable.select().execute():
                bulkRows.append(row)
                if len(bulkRows) > self._batchSize :
                    insert.execute(bulkRows)
                    bulkRows = []
            if len(bulkRows) > 0 :
                insert.execute(bulkRows)
