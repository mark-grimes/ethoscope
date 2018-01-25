__author__ = 'ryanfpage'

import MySQLdb
import os
import logging
import traceback


class MySQLdbCSVWriter(object):

    def __init__(self,
                            remote_db_name="ethoscope_db",
                            remote_host="localhost",
                            remote_user="ethoscope",
                            remote_pass="ethoscope",
                            ):
        """

        A class to dump the current data base into a csv file. Connects to MySQL server and pulls the data in the
        ROI tables out, writing them to a txt file. The columns are tab formatted.


        :param remote_db_name: the name of the database running locally.
        :param remote_host: the ip of the database - localhost will be fully test remote testing to follow
        :param remote_user: the user name for the database
        :param remote_pass: the password for the database
        :param overwrite: whether the destination file should be overwritten. If False, data are appended to it


        """
        try:
            self._remote_host = remote_host
            self._remote_user = remote_user
            self._remote_pass = remote_pass
            self._remote_db_name = remote_db_name

        except Exception as e:
            raise

    def write_roi_tables(self, filepath, overwrite, nows, continuous):
        """
        Fetch new ROI tables from mysql database and populate in a textfile
        """

        csv_file_name = filepath+"/"+self._remote_db_name + ".txt"
        # print ("Filename:", self._csv_file_name)


        if os.path.isfile(csv_file_name):
            # print ("Filename:", self._csv_file_name)
            if overwrite:
                logging.info("MySQLdbCSVWriter trying to remove old database")
                try:
                    os.remove(csv_file_name)
                    logging.info("MySQLdbCSVWriter success")
                except OSError as e:
                    logging.warning(e)
                    pass
        irow = 0
        rowgen = self.enumerate_roi_tables()
        logging.info("MySQLdbCSVWriter writing to file:", csv_file_name)
        try:
            with open(csv_file_name,"a") as f:
                for row in rowgen:
                    f.write(row)
                    f.write("\n")
                    if (continuous is not True) and (irow < nows):
                        irow = irow + 1
                    elif continuous is not True:
                        break
            f.close()
            logging.info('MySQLdbCSVWriter closing file')
        except Exception as e:
            print e
            raise



    def enumerate_roi_tables(self):
        """
        Returns an iterator that will give each row. Should be less resource intensive for larger
        databases when you want to do something with the data other than dump to a file, e.g. serve
        directly over Bottle.
        """
        # Developer note: This is intentionally all in one function rather than delegating to
        # another (a la "_update_one_roi_table") because Bottle cannot serve from nested iterables.
        src = MySQLdb.connect(host=self._remote_host, user=self._remote_user,
                                         passwd=self._remote_pass, db=self._remote_db_name)

        command = "SELECT roi_idx FROM ROI_MAP"
        cur = src.cursor()
        cur.execute(command)
        rois_in_src = set([c[0] for c in cur])
        rowiter = 0
        for i in rois_in_src :
            src_cur = src.cursor()
            src_command = "SELECT * FROM ROI_%i" % (i)
            src_cur.execute(src_command)
            if rowiter == 0:
                field_names = [j[0] for j in src_cur.description]
                row_names = "\t".join([name for name in field_names])
                row_names += "\t"+"roi"+"\n"
                rowiter = rowiter + 1
                yield row_names
            for sc in src_cur:
                row = "\t".join(["{0}".format(val) for val in sc])
                row += "\t"+str(i)+"\n"
                yield row

        src.close()
