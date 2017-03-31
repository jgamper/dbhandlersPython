# import socks
# import socket
#
# socks.setdefaultproxy(proxy_type=socks.PROXY_TYPE_SOCKS5, addr="127.0.0.1", port=9050)
# socket.socket = socks.socksocket

## RUSSELL
# from russellhandler import RussellHandler
# path_to_db = '/media/jevjev/FC5891D85891924E/FINDB'
# db_name = 'russell3000.db'
#
# with RussellHandler(path_to_db, db_name) as obj:
# 	gen = obj.initDB()
# print('Done')

## SPX FROM google
from spxhandler import SpxHandler
path_to_db = '/media/jevjev/FC5891D85891924E/FINDB'
db_name = 'spxgoogle.db'

with SpxHandler(path_to_db, db_name) as obj:
	obj.initDB()
print('Done')
