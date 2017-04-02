from sqlhandler import SqlHandler
import urllib.request
from bs4 import BeautifulSoup
from getgoogledata import getUsStocks
import pandas.io.sql as pdsql
import time
import random
import pandas as pd
from datetime import datetime
# HELPER FUNCTIONS FOR SpxHandler object
def scrapeCurrent500():
    """
    Scrapes current holdings of the S&P500 from wikipedia
    """
    SITE = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    page = urllib.request.urlopen(SITE)
    soup = BeautifulSoup(page, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            tickers.append(str(col[0].string.strip())) # tickers.append(ticker = str(col[0].string.strip()))
    return tickers

class SpxHandler(SqlHandler):

    def __init__(self, path_to_db, db_name):
        super(SpxHandler, self).__init__(path_to_db, db_name)

    def regUpdate(self, day=datetime.now().day):
        """
        Method to update the database from the last Date entry in the database
        """
        self.logger.info('Runnig regUpdate')
        tickers_wiki = scrapeCurrent500()
        tickers_db = self.getTables()
        for ticker in tickers_wiki:
            if ticker == 'BRK-B':
                ticker = 'BRK.B'
            if ticker == 'BF-B':
                ticker = 'BF.B'
            # If ticker is already in database
            if ticker in tickers_db:
                # get latest date
                self.logger.info('Downloading this stock {}'.format(ticker))
                date = self.getLatestDate(ticker)
                if date.day != day:
                    # print('Apperently i have not been updated {}'.format(ticker))
                    data = getUsStocks(ticker, 60, 4)
                    sleep_time =  random.randint(10, 80)
                    self.logger.info('Sleeping for: {} seconds'.format(sleep_time))
                    time.sleep(sleep_time)# so google doesnt get mad
                    # append data only past the last Date value in DB
                    date = pd.DatetimeIndex([date])[0]
                    data = data[data['Date'] > date]
                    pdsql.to_sql(data, name = ticker, con = self.con, index=False, if_exists='append')
                    self.logger.info('Uploaded this stock {}, starting this date {}'.format(ticker, date))
                self.logger.info('Already updated {}'.format(ticker))
            else:
                self.logger.info('Some exception {}'.format(ticker))
                # if ticker not in db then just get the whole thing
                data = getUsStocks(ticker, 60, 4)
                pdsql.to_sql(data, name = ticker, con = self.con, index=False, if_exists='append')
                self.logger.info('Uploaded this stock {}'.format(ticker))
                sleep_time =  random.randint(10, 80)
                self.logger.info('Sleeping for: {} seconds'.format(sleep_time))
                time.sleep(sleep_time)# so google doesnt get mad
        self.logger.info('Completed regUpdate')

    def initDB(self):
        """
        Method to populate spx.db for the first time
        """
        self.logger.info('Populating spx.db for the first time')
        # Get current holdings of the SPX
        tickers = scrapeCurrent500()
        for ticker in tickers:
            data = getUsStocks(ticker, 60, 4)
            pdsql.to_sql(data, name = ticker, con = self.con, index=False, if_exists='append')
            self.logger.info('Uploaded this stock {}'.format(ticker))
            sleep_time =  random.randint(10, 80)
            self.logger.info('Sleeping for: {} seconds'.format(sleep_time))
            time.sleep(sleep_time)# so google doesnt get mad
        self.logger.info('Completed populating spx.db for the first time')
