

# function to replace _dl_mult_symbols in padnas datareader module
def _dl_mult_symbols(symbols, start, end, interval, chunksize, retry_count, pause,
                     method):
    stocks = {}
    failed = []
    passed = []
    for sym_group in _in_chunks(symbols, chunksize):
        for sym in sym_group:
            while True:
                try:
                    stocks[sym] = method(sym, start, end, interval, retry_count, pause)
                    passed.append(sym)
                except IOError:
                    print('This guy failed: ', sym)
                    sym = str(input('Alternative symbol to try? '))
                    if sym == 'PASS':
                        break
                    else:
                        continue
                break
#                    warnings.warn('Failed to read symbol: {0!r}, replacing with '
#                                  'NaN.'.format(sym), SymbolWarning)
#                    failed.append(sym)

    if len(passed) == 0:
        raise RemoteDataError("No data fetched using "
                              "{0!r}".format(method.__name__))
    try:
        if len(stocks) > 0 and len(failed) > 0 and len(passed) > 0:
            df_na = stocks[passed[0]].copy()
            df_na[:] = np.nan
            for sym in failed:
                stocks[sym] = df_na
        return Panel(stocks).swapaxes('items', 'minor')
    except AttributeError:
        # cannot construct a panel with just 1D nans indicating no data
        raise RemoteDataError("No data fetched using "
                              "{0!r}".format(method.__name__))
