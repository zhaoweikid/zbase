# coding: utf-8
import os, sys
import time
from zbase.server import client
from zbase.thriftclient.payprocessor import PayProcessor

n = 1

try:
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
except:
    n = 1

server = {'addr':('127.0.0.1', 7200), 'timeout':1000}

for i in range(0, n):
    x = client.ThriftClient(server, PayProcessor, framed=True)
    print 'ping:', i, x.ping()
    x.close()



