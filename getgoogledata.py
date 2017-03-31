import urllib.request
import datetime as dt
import pandas as pd

def getGoogleData(symbol, period, window, exch = 'NYSE'):
    """
    symbol = tickers
    period = sample frequency in seconds
    window = number of days in days
    exch = exchange to get data from
    Borrowed from https://github.com/jwfu/get_google_data
    For more information on Financial Data provided by google see:
    https://www.google.com/intl/en/googlefinance/disclaimer/
    """
    url_root = ('http://www.google.com/finance/getprices?i='
                + str(period) + '&p=' + str(window)
                + 'd&f=d,o,h,l,c,v&df=cpct&x=' + exch.upper()
                + '&q=' + symbol.upper())
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
    response = opener.open(url_root)
    data=response.read().decode().split('\n')       #decode() required for Python 3
    data = [data[i].split(',') for i in range(len(data)-1)]
    header = data[0:7]
    data = data[7:]
    header[4][0] = header[4][0][8:]                           #get rid of 'Columns:' for label row
    df=pd.DataFrame(data, columns=header[4])

    ind=pd.Series(len(df))
    for i in range(len(df)):
        if df['DATE'].ix[i][0] == 'a':
            anchor_time = dt.datetime.fromtimestamp(int(df['DATE'].ix[i][1:]))  #make datetime object out of 'a' prefixed unix timecode
            ind[i]=anchor_time
        else:
            ind[i] = anchor_time +dt.timedelta(seconds = (period * int(df['DATE'].ix[i])))
    df.index = ind

    df=df.drop('DATE', 1)

    for column in df.columns:                #bad implementation because to_numeric is pd but does not accept df
        df[column]=pd.to_numeric(df[column])
    df.index.names = ['Date']
    df.columns = ['Close', 'High', 'Low', 'Open', 'Volume']
    return df.reset_index()

def getUsStocks(symbol, period, window):
    """
    Tries NYSE Exchange first if error, then tries NASDAQ
    """
    exchange = 'NYSE'
    while True:
        try:
            # Tries default NYSE
            data = getGoogleData(symbol, period, window, exch=exchange)
        except ValueError:
            exchange = 'NASDAQ'
            continue
        break
    return data

def getUsEtf(symbol, period, window):
    """
    Tries NYSEARCA exchange first, if error tries NASDAQ
    """
    exchange = 'NYSEARCA'
    while True:
        try:
            # Tries default NYSE
            data = getGoogleData(symbol, period, window, exch=exchange)
        except ValueError:
            exchange = 'NASDAQ'
            continue
        break
    return data

# if __name__ == '__main__':
#
#     print(getUsStocks('AMZN', 60, 1))
