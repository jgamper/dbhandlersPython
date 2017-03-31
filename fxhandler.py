from sqlhandler import SqlHandler
import urllib.request
import datetime
import pandas.io.sql as pdsql

####################################### URL FOR DOWNLOAD
url = 'http://www.netfonds.no/quotes/posdump.php?date={date}&paper={name}.FXSB&csv_format=csv'
#######################################

# HELPER FUNCTIONS FOR FxHandler object
def getWebCSV(url, date, name):
    data = pd.read_csv(urllib.request.urlopen(url.format(date=date,name=name)))
    data = data.drop_duplicates('time')
    dat_func = lambda x: datetime.datetime.strptime(x, '%Y%m%dT%H%M%S')
    data['time'] = data['time'].apply(dat_func)
    data.index = data['time']
    data.drop(data.coluDmns[[0,2,3,5,6]], axis=1, inplace=True)
    bid = data['bid'].resample('1Min').ohlc().bfill()
    bid.rename(columns={'open':'bid_open', 'high':'bid_high', 'low':'bid_low', 'close':'bid_close'},
                       inplace = True)
    ask =  data['offer'].resample('1Min').ohlc().bfill()
    ask.rename(columns={'open':'ask_open', 'high':'ask_high', 'low':'ask_low', 'close':'ask_close'},
                       inplace = True)
    data = pd.concat([ask, bid], axis=1)
    data.index.names=['Date']
    return data.reset_index()

# FxHandler object
class FxHandler(SqlHandler):

    def __init__(self, path_to_db, db_name):
        continue

    def regUpdate():
        """
        Method to update the database from the last Date entry in the database
        """
        self.logger.info('Updating the fx database')
        dates = [str(self.getLatestDate('EURGBP')).split()[0].replace('-','') for date in list(pd.bdate_range(date, today-datetime.timedelta(days=1)))]
        names = self.getTables()
        for name in names:
            data = getWebCSV(url, dates[0], name)
            for i in range(1, len(dates)):
                data.append(getWebCSV(url, dates[i], name))
            pdsql.to_sql(data, name = name, con = self.con, index=False, if_exists='append')
            self.logger.info('Updated this fx pair {}'.format(name))

    def initDB(dates_list):
        """
        Populates the database for the first time
        dates_list has to look like this ['20170113', '20170116', '20170117'] and have only bussines days!
        """
        self.logger.info('Populating fx.db for the first time')
        names_list = ['EURGBP', 'EURJPY', 'EURUSD', 'USDCHF', 'USDJPY', 'USDCAD', 'GBPUSD', 'EURAUD', 'EURCAD']
        for name in names_list:
            for i in range(len(dates_list)):
                if i == 0:
                    data = getWebCSV(url, dates[i], name)
                else:
                    data = data.append(getWebCSV(url, dates[i], name))
            pdsql.to_sql(data, name = name, con = self.con, index=False, if_exists='append')
            self.logger.info('Uploaded this fx pair {}'.format(name))
