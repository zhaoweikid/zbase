3# coding: utf-8
import os, sys
HOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(HOME)
if sys.argv[1].find('server') > 0:
    from gevent import monkey; monkey.patch_all()
import traceback, time
from zbase.base import logger
log = logger.install('stdout')
from zbase.server import rpc
import errno
import functools
from tornado import ioloop
import socket
import logging

PORT = 20000

serial_mod = 'json'

def haha(name):
    time.sleep(1)
    return 'haha '+name


def server():
    rpc.install(serial_mod)
    rpc.rpc_funcs['haha'] = haha
    #rpc.server(PORT)
    rpc.gevent_server(PORT)

def ws_server():
    rpc.install(serial_mod)
    rpc.rpc_funcs['haha'] = haha
    rpc.websocket_server(PORT, '/')

def client():
    rpc.install(serial_mod)
    c = rpc.RPCClient(('127.0.0.1', PORT), 5000)
    c.flag[rpc.FLAG_WAIT] = rpc.FLAG_WAIT_NO
    #c = rpc.RPCClient(('127.0.0.1', PORT))
    for i in range(0, 3):
        try:
            print c.haha('bbbb%d' % i)
        except:
            traceback.print_exc()
            
def tclient():
    rpc.install(serial_mod)
    def result(code, ret):
        print 'result:', code, ret

    io_loop = ioloop.IOLoop.instance()
    c = rpc.TornadoRPCClient('127.0.0.1', PORT, max_clients=10)
    #c = rpc.TornadoRPCClient('127.0.0.1', PORT, 1000)
    print 'call:', c.haha('ccccc1', result)
    print 'call:', c.haha('ccccc2', result)
    print 'call:', c.haha('ccccc3', result)
    print 'call:', c.haha('ccccc4', result)
    io_loop.start()

if __name__ == '__main__':
    #log = logger.install('stdout')
    func = globals()[sys.argv[1]]
    func()

