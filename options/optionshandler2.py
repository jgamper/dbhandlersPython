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


arguments = ['SPY', 'AAPL']
months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
          7: 'Jul', 8: 'Aug', 9: 'Se[', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
now = dt.datetime.now()
c_month = months[now.month]
c_day = str(now.day)
c_year = str(now.year)

f = h5.File('/media/jevjev/FC5891D85891924E/FINDB/options_db.h5')  # open database file
year = f.require_group(c_year)  # Make hdf5 group for year
month = year.require_group(c_month)  # Make hdf5 group for month
day = month.require_group(c_day)  # Make hdf5 group for


for i in arguments:
    ticker = day.require_group(i)

    os.system("python3 yahoo_options_python34.py {}".format(i)) # call to a script defined by boyank
    time.sleep(5)
    print('{} completed'.format(i))
    print('Reading downloaded csv')
    df = pd.read_csv("{}.csv".format(i))
    df.drop(['Date', 'Contract Name', 'Bid', 'Ask'], inplace=True, axis=1)

    ## Take care of calls
    df_calls = df[df['Option Type'] == 'CALL']

    expiration_dates = list(df_calls['Expire Date'].unique())
    df_calls = df_calls[['Expire Date', 'Strike', 'Last', 'Change',
                         '%Change', 'Volume', 'Open Interest', 'Implied Volatility']]
    for ex in expiration_dates:
        cur_ex = ex.replace('.', '')
        local_df = df_calls[df_calls['Expire Date'] == ex]
        local_df = local_df[['Strike', 'Last', 'Change',
                             '%Change', 'Volume', 'Open Interest', 'Implied Volatility']]
        calls_dt = ticker.require_dataset('C' + i + cur_ex, local_df.shape, float)
        calls_dt[...] = local_df.astype(np.float32)

    ## Take care of puts
    df_puts = df[df['Option Type'] == 'PUT']

    expiration_dates = list(df_puts['Expire Date'].unique())
    df_puts = df_puts[['Expire Date', 'Strike', 'Last', 'Change',
                         '%Change', 'Volume', 'Open Interest', 'Implied Volatility']]
    for ex in expiration_dates:
        cur_ex = ex.replace('.', '')
        local_df = df_puts[df_puts['Expire Date'] == ex]
        local_df = local_df[['Strike', 'Last', 'Change',
                             '%Change', 'Volume', 'Open Interest', 'Implied Volatility']]
        puts_dt = ticker.require_dataset('P' + i + cur_ex, local_df.shape, float)
        puts_dt[...] = local_df.astype(np.float32)

    print('Finished writting options for this ticker {}'.format(i))

# Remove csv files created
for arg in arguments:
    os.remove(arg+'.csv')
print('Removed the csv files')
f.close()
