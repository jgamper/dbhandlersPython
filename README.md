# Python implementation of web scrappers for financial data

etfhandler, fxhandler, russellhandler and spxhandler scrap data and save it into Sqllite database
while options handlers are supposed to save data into h5 format.

sqlhandler is an abstract base class for etfhandler, fxhandler, russellhandler and spxhandler.

For russellhandler.py to work, need to replace a function _dl_mult_symbols in pandas_datareader module in _utils.py with a function in replacement.py path: /home/jevgenji/anaconda3/pkgs/pandas-datareader-0.2.0-py35_0/lib/python3.5/site-packages/pandas_datareader



