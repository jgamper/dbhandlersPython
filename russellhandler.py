from sqlhandler import SqlHandler
# Modules needed for RussellHandler class
import bs4 as bs
import urllib.request
import requests
import csv
import pandas_datareader.data as web
import pytz
import pandas.io.sql as pdsql
import datetime
import os

# HELPER FUNCTIONS FOR RussellHandler object
def chunks(l, n):
    """
    Helper function to split list of holdings into
    chunks and pass into Pandas data reader in bulks
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]

def getCurrentHoldings(path_to_db):
    """
    Reads current holdings file - holdings.txt
    Returns a list of current holdings
    """
    if os.path.isfile(os.path.join(path_to_db, 'holdings3000.txt')) == False:
        return [0]
    else:
        with open('holdings3000.txt', 'r') as f:
             output = f.readlines()
        return [x.strip() for x in output]

def csvYield(soup):
    """
    Pass csv urls soup. Example:
    source = urllib.request.urlopen('https://www.ishares.com/us/products/239714/ishares-russell-3000-etf')
    soup = bs.BeautifulSoup(source, 'lxml')
    Returns a generator yielding a tuple of sector name and list of tickers and
    names corresponding to that sector
    """
    for url in soup.find_all('a'):
        link = str(url.get('href'))
        if 'csv' in link:
            csv_link = 'https://www.ishares.com' + link
            break
    print(csv_link)
    with requests.Session() as s:
        download = s.get(csv_link)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        internal_list = list(cr)
        yield internal_list[2][1] # yields the date of csv file
        internal = (row for row in internal_list if len(row) == 12)
        # So that it goes nicely sector by sector
        sector_tickers = dict()
        for ticker, name, ass_class, weight, price, shares, mv, nv, sector, *rest in internal:
            sector = str(sector).replace(' ', '_')
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            if sector not in ['nan', 'Cash_and/or_Derivatives']:
#                if '.' in ticker:
#                    ticker.replace('.','-')
                sector_tickers[sector].append((ticker,name))
        for sector in sector_tickers.keys():
            yield (sector, sector_tickers[sector])

def downloadAndUpdate(tickers, start, end, conn):
    """
    Downloads the price data for a given date range and upploads into database
    """
    data = web.DataReader(tickers, 'yahoo', start, end)
    for item in ['Open', 'High','Low']:
            data[item] = data[item]*data['Adj Close']/data['Close']
    data.rename(items={'Open':'open', 'High':'high', 'Low':'low', 'Adj Close':'close', 'Volume':'volume'},
                       inplace = True)
    data.drop(['Close'], inplace = True)
    for i in range(len(list(data.minor_axis))):
        print("Updating {} into database for dates {} to {}".format(data.minor_axis[i], start, end))
        pdsql.to_sql(data.minor_xs(data.minor_axis[i]).reset_index(),
                     name = data.minor_axis[i],
                     con = conn, index=False, if_exists='append')
    print('Finished uploading that many tickers: ', len(tickers))
    return len(tickers)


class RussellHandler(SqlHandler):

    def __init__(self, path_to_db, db_name):
        super(RussellHandler, self).__init__(path_to_db, db_name)

    def updateHoldings(self, new_holdings):
        """
        Updates the holdings.txt after major russell update of the holdings
        """
        self.logger.info('Updated the holdings.txt file')
        with open('holdings3000.txt', 'w') as f:
            for i in new_holdings:
                f.write(i+'\n')

    def regUpdate(self, custom_update=None):
        """
        Updates Russell 3000 database up to the latest available data on yahoo
        By reading the contents of the holdings.txt file in order to know the current
        constituents and update accordingly
        """
        self.logger.info('Starting regular update')
        start = self.getLatestDate('AAPL')
        start += datetime.timedelta(days=1)
        end = datetime.datetime.today().utcnow()
        total_up = 0
        if custom_update == None:
            for chunk in chunks(getCurrentHoldings(self.path), 300): # can change the chunks for memory
                print(chunk)
                total_up += downloadAndUpdate(chunk, start, end, self.con)
        else:
            for chunk in chunks(custom_update, 300): # can change the chunks for memory
                print(chunk)
                total_up += downloadAndUpdate(chunk, start, end, self.con)
        self.logger.info('Finished regular update')
        self.logger.info('Number of holdings in txt: {}'.format(len(getCurrentHoldings(self.path))))
        self.logger.info('Number holdings updated now:{}'.format(total_up))
        return

    def initDB(self):
        """
        Full update with reading the webpage for new holdings
        """
        self.logger.info('FULL UPDATE INITIALIZED')
        source = urllib.request.urlopen('https://www.ishares.com/us/products/239714/ishares-russell-3000-etf')
        soup = bs.BeautifulSoup(source, 'lxml')

        gen = csvYield(soup)

        # should print out the date of the csv file
        print('\n', 'The date of the downloaded csv file is :', next(gen), '\n')
        decision = str(input('Does the date satisfy you? ENTER OR NO '))
        print('\n')

        if decision == 'NO':
            quit()

        current_holdings = set(getCurrentHoldings(self.path))
        new_holdings = set()
        for sector, list_of_tuples in gen:
            if sector != 'Sector': # For some reason this was there?
                if sector != 'Cash_and/or_Derivatives':
                    new_holdings.update(set([ticker[0] for ticker in list_of_tuples]))
        #### WITH TIME THINGS WHICH I USUALLY PASS ON DOWNLOAD COULD BE ADDED BELOW
        new_holdings -= {'P5N994', '-', 'NYLDA', 'LEAP'}
        new_holdings = list(new_holdings)
        #### Remove '.' from tickers, and change other known tickers
        for i in range(len(new_holdings)):
            if '.' in new_holdings[i]:
                new_holdings[i] = new_holdings[i].replace('.','-')
            if new_holdings[i] == 'FCEA':
                new_holdings[i] = 'FCE-A'
            if new_holdings[i] == 'LGFB':
                new_holdings[i] = 'LGF-B'
            if new_holdings[i] == 'LGFA':
                new_holdings[i] = 'LGF-A'
            if new_holdings[i] == 'BRKB':
                new_holdings[i] = 'BRK-B'
        new_holdings = set(new_holdings)
        if len(current_holdings) < 10:
            # the database has not been populated, serves as initDB part
            end_today = datetime.datetime.today().utcnow()
            start = end_today - datetime.timedelta(days=60*3)
            self.logger.info('Populating database for the first time')
            for chunk in chunks(list(new_holdings), 300):
                downloadAndUpdate(chunk, start, end_today, self.con)
            self.updateHoldings(list(new_holdings))
            self.logger.info('Finshed populating database for the first time')
        else:
            # serves as fullUpdate part
            # update intersection of new holdings and
            intersection = list(new_holdings & current_holdings)
            # self.regUpdate(custom_update = intersection)
            # Get all the new ones
            new = new_holdings - current_holdings
            del current_holdings
            del new_holdings
            new = list(new)
            if not new:
                print('Nothing new to add')
                quit()
            end = datetime.datetime.today().utcnow()
            start = end - datetime.timedelta(days=60)
            for chunk in chunks(new, 300):
                downloadAndUpdate(chunk, start, end, self.con)
            print('These guys have been added:')
            print(new, '\n')
            self.updateHoldings(new+intersection)
            print('Holdings txt updated, done')
            self.logger.info('FULL UPDATE FINISHED')
            return

# if __name__ == '__main__':
    # # Testing
    # path_to_db = '/media/jevjev/FC5891D85891924E/FINDB'
    # db_name = 'russell3000.db'
    #
    # with RussellHandler(path_to_db, db_name) as obj:
    #     gen = obj.
    # print('Done')
