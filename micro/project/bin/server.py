# coding: utf-8
import os, sys
HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(HOME, 'conf'))
QFNAME = os.environ.get('QFNAME')
import config
from zbase.base import logger
log = logger.install(config.LOGFILE)

from zbase.server import thriftserver
from zbase.thriftclient import *
from zbase.micro import core
import main

if 'QFNAME' in os.environ:
    config.QFNAME = os.environ['QFNAME']


handler = None
for k,v in main.__dict__.iteritems():
    if k.endswith('Handler'):
        if issubclass(v, core.Handler):
            handler = v
            break
if not handler:
    print 'Not found thrift handler in main.py !!! class must named XXXXHandler !!!'
    sys.exit(-1)

server = thriftserver.ThriftServer(handler.define, handler, (config.HOST, config.PORT), config.PROCS)
server.forever()

