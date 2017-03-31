from sqlhandler import SqlHandler
import urllib.request
from bs4 import BeautifulSoup
from getgoogledata import getUsEtf
import pandas.io.sql as pdsql
import time
# list of ETFS FOR SpxHandler object
the_large_etfs = [ "SPY",  # SPDR S&P 500 ETF Trust
                   "QQQ",  # PowerShares QQQ Trust Series
                   "DIA",  # SPDR Dow Jones Indust Avg
                   "MDY",  # SPDR S&P MidCap 400
                   "IWB",  # IShares Russell 1000
                   "IWM",  # IShares Russell 2000 (Small to Mid)
                   "IWV",  # IShares Russell 3000 (Entire Mkt)
                   "EZU",  # IShares MSCI Eurozone
                   "EFA",  # IShares MSCI EAFE (Dev markets outside US&Canda)
                   "EEM",  # IShares MSCI Emerging Mkts
                   "BKF",  # IShares MSCI BRIC
                   "ILF",  # IShares MSCI Latin America 40
                   "FM",   # IShares MSCI Frontier 100
                   "EPP",  # IShares MSCI Pacific ex Japan
                   "EWJ",  # IShares MSCI Japan
                   "SCJ",  # IShares MSCI Japan Small-Cap
                   "ECH",  # IShares MSCI Chile Capped
                   "EIDO", # IShares MSCI Indonesia
                   "EIRL", # IShares MSCI Ireland
                   "EIS",  # IShares MSCI EIS
                   "ENZL", # IShares MSCI New Zealand
                   "EPHE", # IShares MSCI Philippines
                   "EPOL", # IShares MSCI Poland Capped
                   "EPU",  # IShares MSCI All Peru Capped
                   "ERUS", # IShares MSCI Russia
                   "EWC",  # IShares MSCI Canada
                   "EWD",  # IShares MSCI Sweden
                   "EWG",  # IShares MSCI Germany
                   "EWH",  # IShares MSCI Honk Kong
                   "EWI",  # IShares MSCI Italy Capped
                   "EWK",  # IShares MSCI Belgium Capped
                   "EWL",  # IShares MSCI Switzerland Capped
                   "EWM",  # IShares MSCI Malaysia
                   "EWN",  # IShares MSCI Netherelads
                   "EWO",  # IShares MSCI Austria Capped
                   "EWP",  # IShares MSCI Spain Capped
                   "EWQ",  # IShares MSCI France
                   "EWS",  # IShares MSCI Singapore
                   "EWT",  # IShares MSCI Taiwan
                   "EWU",  # IShares MSCI United Kingdom
                   "EWW",  # IShares MSCI Mexico Capped
                   "EWY",  # IShares MSCI South Korea Capped
                   "EWZ",  # IShares MSCI Brazil Capped
                   "EZA",  # IShares MSCI South Africa
                   "THD",  # IShares MSCI Thailand Capped
                   "TUR",  # IShares MSCI Turkey
                   "INDA", # Ishares MSCI India
                   "MCHI", # IShares MSCI China
                   "IAU",  # IShares MSCI Gold Trust
                   "SLV",  # IShares Silver Trust
                   "GDX",  # VanEck Vectors Gold Miners
                   "USO",  # United States Oil Fund
                   "DBA",  # PowerShares Agriculture Fund
                   "DBB",  # PowerShares
                   "GSG",  # IShaeres S&P GSCI Commodity Index
                   "XLB",  # Materials Select Sector SPDR
                   "XLE",  # Energy Select Sector SPDR
                   "XLF",  # Financial Select Sector SPDR
                   "XLI",  # Industrial Select Sector SPDR
                   "XLK",  # Technology Select Sector SPDR
                   "XLP",  # Consumer Staples Select Sector SPDR
                   "XLU",  # Utilities Select Sector SPDR
                   "XLV",  # Health Care Select Sector SPDR
                   "IYZ",  # Telecommunications U.S IShares
                   "XLY",  # Consumer Discretionary Select Sector SPDR
                   "SHY",  # 1-3 Year Treasury Bond
                   "IEI",  # 3-7 Year Treasury Bond
                   "IEF",  # 7-10 Year Treasury Bond
                   "TLH",  # 10-20 Year Treasury Bond
                   "TLT",  # 20+ Year Treasury Bond
                   "TIP",  # TIPS Bond
                   "LQD",  # Invest Grade Corp Bond
                   "HYG",  # High Yield Bond
                   "AGG",  # Core U.S. Aggregate Bond
                   "EMB",  # USD Emerging Mkts Bond
                   "BWX",  # International Treasury Bond
                   "RWR",  # Dow Jones Reit ETF
                   "IYR",  # IShares US Real Estate
                   "REM",  # IShares Mortgage Real Estate
                   "UUP",  # US Dollar Up
                   "FXA",  # Australia Currency
                   "FXC",  # Canada Currency
                   "FXB",  # British Pound Currency
                   "FXE",  # Euro Currency
                   "FXY",  # Japan Currency
                   "FXS",  # Sweden Currency
                   "FXF"]   # Switzerland Currency

class EtfHandler(SqlHandler):

    def __init__(self, path_to_db, db_name):
        continue

    def regUpdate():
        """
        Method to update the database from the last Date entry in the database
        """
        self.logger.info('Runnig regUpdate')
        tickers = self.getTables()
        for ticker in tickers:
                # get latest date
                date = self.getLatestDate(ticker)
                data = getUsEtf(ticker, 60, 80)
                sleep_time =  random.randint(10, 80)
                self.logger.info('Sleeping for: {} seconds'.format(sleep_time))
                time.sleep(sleep_time)# so google doesnt get mad
                # append data only past the last Date value in DB
                date = pd.DatetimeIndex([date])[0]
                data = data[data['Date'] > date]
                pdsql.to_sql(data, name = ticker, con = self.con, index=False, if_exists='append')
                self.logger.info('Upgraded this stock {}'.format(ticker))
                time.sleep(5) # so google doesnt get mad
        self.logger.info('Completed regUpdate')

    def initDB():
        """
        Method to populate spx.db for the first time
        """
        self.logger.info('Populating spx.db for the first time')
        # Get current holdings of the SPX
        tickers = the_large_etfs
        for ticker in tickers:
            data = getUsEtf(ticker, 60, 80)
            pdsql.to_sql(data, name = ticker, con = self.con, index=False, if_exists='append')
            self.logger.info('Uploaded this stock {}'.format(ticker))
            sleep_time =  random.randint(10, 80)
            self.logger.info('Sleeping for: {} seconds'.format(sleep_time))
            time.sleep(sleep_time)# so google doesnt get mad # so google doesnt get mad
        self.logger.info('Completed populating spx.db for the first time')
