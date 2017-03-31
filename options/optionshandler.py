import datetime as dt
import pandas as pd
import numpy as np
import h5py as h5
import os
import time


####### TO DO! #######
# 1) Add logger
# 2) Sometimes when it tries to download Yahoo blocks it, change the options code
# so that it would catch the error, delete csv and try again
# 3) Add function to delete CSV files after updating the db


arguments = ['SPY', 'AAPL']
months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
          7: 'Jul', 8: 'Aug', 9: 'Se[', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
now = dt.datetime.now()
c_month = months[now.month]
c_day = str(now.day)
c_year = str(now.year)

f = h5.File('options_db.h5')  # open database file
year = f.require_group(c_year)  # Make hdf5 group for year
month = year.require_group(c_month)  # Make hdf5 group for month
day = month.require_group(c_day)  # Make hdf5 group for


for i in arguments:
    ticker = day.require_group(i)

    os.system("python3 yahoo_options_python34.py {}".format(i))
    time.sleep(5)
    print('{} completed'.format(i))
    print('Reading downloaded csv')
    df = pd.read_csv("{}.csv".format(i))
    df.drop(['Date', 'Contract Name', 'Bid', 'Ask'], inplace=True, axis=1)

    df_calls = df[df['Option Type'] == 'CALL']
    df_calls = df_calls[['Expire Date', 'Strike', 'Last', 'Change',
                         '%Change', 'Volume', 'Open Interest', 'Implied Volatility']]
    conversion = lambda x: int(x.replace('.',''))
    df_calls['Expire Date'] = df_calls['Expire Date'].apply(conversion)
    calls_dt = ticker.require_dataset('C' + i, df_calls.shape, float)
    calls_dt[...] = df_calls.astype(np.float32)

    df_puts = df[df['Option Type'] == 'PUT']
    df_puts = df_puts[['Expire Date', 'Strike', 'Last', 'Change',
                         '%Change', 'Volume', 'Open Interest', 'Implied Volatility']]
    df_puts['Expire Date'] = df_puts['Expire Date'].apply(conversion)
    puts_dt = ticker.require_dataset('P' + i, df_puts.shape, float)
    puts_dt[...] = df_puts.astype(np.float32)
    print('Finished writting options for this ticker {}'.format(i))

# Remove csv files created
for arg in arguments:
    os.remove(arg+'.csv')
print('Removed the csv files')
f.close()
