import pandas as pd
import datetime
import sqlite3
import os
import logging
from abc import ABCMeta, abstractmethod

def retLogger(path_to_db, db_name):
    """
    Returns logger for the SqlHandlers, creates a log file if there is none
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(filename=os.path.join(path_to_db, db_name.replace('.db', '.log')))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def testDBExistence(path_to_db, db_name):
    if os.path.isfile(os.path.join(path_to_db, db_name)) == False:
        raise MyException("Wrong path_to_db or db_name")
    else:
        return


class MyException(Exception):
    pass


class SqlHandler(metaclass=ABCMeta):
    """
    TO DO:
    1) Implement proper return N data points method!!! Needed for some backwards looking forecast algorithms
    2) Implement proper generator method to catch the StopIteration exception and pause before the data is futher updated and then generate from that last value
    StackOverflow link: http://stackoverflow.com/questions/2058894/python-how-to-i-check-if-last-element-has-been-reached-in-iterator-tool-chain
    Could I do it with @staticmethod or classmethod?

    SQL handler is a base class providing an interface for sql handlers.
    The goal of the (derived) DatabaseHandler object is to output and update financial time series database.

    This should provide general guidance and functionality for the database management and computation on the
    database.

    *To begin using the SqlHandler functionality use with statement. I thought that way it will ensure that the connection
    with the database is closed after the user is done using it, and will manage the memory more efficiently. Since __exit__
    method is called regardless of whether or not exception occurs.

    SqlHandler provides logging functionality to track the action on the database.
    """

    # Instantiates for a given database
    def __init__(self, path_to_db, db_name):
        """
        Inherit this way:
            class Russell3000(SqlAbstractHandler):
        """
        # Check if db exists there raise exception if not:
        try:
            testDBExistence(path_to_db, db_name)
        except MyException:
            print('The database does not exist in current path')
            decision = str(input('Would you like to create one? NO YES '))
            if decision == 'NO':
                quit()

        self.name = db_name
        self.path = path_to_db
        self.path_full = os.path.join(path_to_db, db_name)
        self.con = sqlite3.connect(self.path_full)
        self.cursor = self.con.cursor()
        # get logger for updating the log file, also creates the log file if does not exist
        self.logger = retLogger(self.path, self.name)
        self.logger.info('Instantiated a class and connected to {}'.format(self.path_full))

    # BASE FUNCTIONALITIES
    def getTables(self):
        """
        Returns a list of tables in the database
        """
        self.logger.info('Retrieving the names of all the tables in')
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [table[0] for table in self.cursor.fetchall()]

    def getLatestDate(self, table_name):
        """
        Returns the date in the latest row in a table
        last_date_in_db = object.getLatestDate('AAPL')
        Returns the date.as datetime.datetime object
        """
        self.logger.info('Retrieving the latest date for {} in database'.format(table_name))
        self.cursor.execute('SELECT max(Date) FROM {}'.format(table_name))
        return datetime.datetime.strptime(self.cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S")

    def getFirstDate(self, table_name):
        """
        Returns the date in the latest row in a table
        last_date_in_db = object.getLatestDate('AAPL')
        Returns the date.as datetime.datetime object
        """
        self.logger.info('Retrieving the first date for {} in database'.format(table_name))
        self.cursor.execute('SELECT min(Date) FROM {}'.format(table_name))
        return datetime.datetime.strptime(self.cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S")

    def getLastRow(self, table_name):
        """
        Returns the last row of a table_name table
        """
        self.logger.info('Retrieving the last row for {} in database'.format(table_name))
        self.cursor.execute('SELECT * FROM {} WHERE Date=?'.format(table_name),(self.getLatestDate(table_name),))
        data = pd.DataFrame(self.cursor.fetchall())
        data.columns = [description[0] for description in self.cursor.description]
        return data.set_index('Date')

    def getNDatesLastRows(self, table_name, N):
        """
        Returns the data frame from last row to N Dates back
        """
        self.logger.info('Retrieving {} rows from {}'.format(N, table_name))
        latest_date = self.getLatestDate(table_name)
        start_date = latest_date - datetime.timedelta(days=N)
        self.cursor.execute('SELECT * FROM {} WHERE Date BETWEEN ? AND ?;'.format(table_name), (start_date, latest_date))
        data = pd.DataFrame(self.cursor.fetchall())
        data.columns = [description[0] for description in self.cursor.description]
        return data.set_index('Date')

    def generateRows(self, table_name):
        """
        Generator yielding new row from the database table, TO BE USED FOR EVENT-DRIVEN BACKTESTING
        """
        self.logger.info('Initialized a row generator for table: {}'.format(table_name))
        self.cursor.execute('SELECT * FROM {}'.format(table_name))
        for row in self.cursor.fetchall():
            data = pd.DataFrame([row])
            data.columns = [description[0] for description in self.cursor.description]
            data.set_index('Date')
            yield data

    # abstactmethods to be overwritten in subsequent objects
    @abstractmethod
    def regUpdate():
        """
        Regular Update method, which should be implemented in all subsequent objects
        """
        raise NotImplementedError("Should implement regUpdater()")

    @abstractmethod
    def initDB():
        """
        Method to initialize the database if it did not exist,
        which should be implemented in all subsequent objects.
        Has to start as soon as the database is implemented for the first time,
        after the user has confirmed to continue
        """
        raise NotImplementedError("Should implement initDB() method")

    # TECHNICAL FUNCTIONALITIES:
    def closeCon(self):
        """
        Closes connection with the database
        """
        self.con.close()
        self.logger.info('CLOSED connection with the database')

    def __repr__(self):
        self.logger.info('__repr__ method used')
        return 'This object is connected to {}'.format(self.path_full)

    def __str__(self):
        self.logger.info('__str__ method used')
        return 'This object is connected to {}'.format(self.path_full)

    def __enter__(self):
        self.logger.info('__enter__ method used')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.info('__exit__ method used')
        self.closeCon()
