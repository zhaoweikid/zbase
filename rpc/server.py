# coding: utf-8
import os, sys
import time, datetime
import traceback
import socket
import types, re
import logging
log = logging.getLogger()

from zbase.base import logger

import tornado
from tornado import ioloop
from tornado.tcpserver import TCPServer
import msgpack, struct
import threading, random
import signal
import packer


class RPCFormatError (Exception):
    pass


class RPCField:
    def __init__(self, restr, totype=None):
        self.totype = totype
        self.regex  = restr

        if type(restr) in (types.UnicodeType, types.StringType):
            self.regex = re.compile(restr)
    
    def match(self, s):
        if not self.regex.match(s):
            return None
        if not self.totype:
            return s
        if self.totype in (types.IntType, types.LongType):
            return int(s)
        if self.totype == types.FloatType:
            return float(s)


def rpc_validator(**kwformat):
    def f(func):
        def _(self, stream, data):
            newdata = {}
            for k,v in data.iteritems():
                fmt = kwformat.get(k)
                if fmt:
                    log.debug('validator: %s %s', k, v)
                    ret = fmt.match(v)
                    if not ret:
                        log.info('field %s error: %s', k, v)
                        raise RPCFormatError
                    newdata[k] = ret

            return func(self, stream, newdata)
        return _ 
    return f

class RPCHandler:
    def handle(self, stream, data):
        log.info('handle data:%s', data)

class RPCConnection (object):
    conn = {}

    def __init__(self, stream, address, config):
        self.stream  = stream
        self.address = address
        self.config  = config

        key = '%s:%d' % self.address
        self.connkey = key
        RPCConnection.conn[key] = self

        self.stream.read_bytes(4, self.on_header)
        self.stream.set_close_callback(self.on_close)

        self.handler = config.handler()
        #log.info('RPCConnection init %s:%d' % self.address)

    def __del__(self):
        log.info('RPCConnection deleted')

    def on_close(self):
        try:
            del RPCConnection.conn[self.connkey]
        except:
            log.info(traceback.format_exc())
        self.stream = None
        log.warn('server conn close:%s', self.address)

    def on_header(self, data):
        #log.debug('header data:%s', repr(data))
        try:
            bodylen = struct.unpack('I', data)[0]
        except:
            log.warning('package header error:%s', repr(data))
            log.warning(traceback.format_exc())
            self.stream.close()
            return

        if bodylen > self.config.max_package_size:
            log.warning('package size too big: %d>%d', 
                    bodylen, self.config.max_package_size)
            self.stream.close()
            return

        #log.debug('bodylen:%d', bodylen)
        if bodylen > 0:
            self.stream.read_bytes(bodylen, self.on_body)
        else:
            log.debug('bodylen error:%d', bodylen)

    def on_body(self, data):
        starttm = time.time()
        unpackdata = packer.loads(data)
        ver, seqid, name, args = unpackdata

        retobj = [packer.VERSION, seqid, 0, None]
        retval = None
        try:
            namex = name.split('.')
            if namex[0] == self.config.name:
                namex.pop(0)
            m = self.handler
            for name in namex:
                m = getattr(m, name)
            retval = m(self.stream, args)
            #log.debug('retval:%s', repr(retval))
        except Exception, e:
            log.warning(traceback.format_exc())
            retobj[2] = -1
            retobj[3] = str(e)
            log.info('server=%s func=%s addr=%s:%d time=%d args=%s ret=%s err=%s', 
                    self.config.name, name, 
                    self.address[0], self.address[1], 
                    int((time.time()-starttm)*1000000),
                    str(args), str(retval), str(e))
        else: 
            log.info('server=%s func=%s addr=%s:%d time=%d args=%s ret=%s', 
                    self.config.name, name, 
                    self.address[0], self.address[1], 
                    int((time.time()-starttm)*1000000),
                    str(args), str(retval))
            retobj[3] = retval

        #if not retobj is None:
        #log.debug('retobj:%s', repr(retobj))
        try:
            s = packer.dumps_header(retobj)
            #log.debug('write:%s', repr(s))
            self.stream.write(s)
        except:
            log.warning(traceback.format_exc())



class RPCServer (TCPServer):
    def handle_stream(self, stream, address):
        log.debug('new stream:%s', address)
        try:
            RPCConnection(stream, address, self.config) 
        except:
            log.warning(traceback.format_exc())

def serve(config):
    io_loop = ioloop.IOLoop.instance()
    server = RPCServer(io_loop)
    server.config = config
    server.bind(config.addr[1])
    server.start()
    log.debug('server started')
    io_loop.start()

def report_nameserver(config):
    def start():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        sock.settimeout(config.ns_timeout/1000.0)

        while True:
            for nsaddr in config.ns_addrs:
                retobj = None
                try:
                    seqid = random.randint(1, 10000)
                    obj = [seqid, 'r', config.name, config.addr]
                    data = packer.dumps(obj)
                    n = sock.sendto(data, nsaddr)
                    data, clientaddr = sock.recvfrom(512)
                    retobj = packer.loads(data)
                    log.info('server=ns func=report addr=%s:%d ret=%s', nsaddr[0], nsaddr[1], retobj)
                except Exception, e:
                    log.warning(traceback.format_exc())
                    err = repr(str(e))
                    log.info('server=ns func=report addr=%s:%d ret=%s err=%s', nsaddr[0], nsaddr[1], retobj, err)

            time.sleep(config.ns_report_interval/1000.0)

    t = threading.Thread(target=start, args=())
    t.start()

def run(config):
    try:
        if hasattr(config, 'ns_addrs'):
            report_nameserver(config)
        serve(config)
    except KeyboardInterrupt:
        log.warning('kill me')
        os.kill(os.getpid(), signal.SIGTERM)

if __name__ == '__main__':
    class ZWHandler:
        def __init__(self):
            pass

        def ping(self, stream, data):
            log.debug('recv: %s, return pong', data)
            return 'pong'

    class Config:
        addr             = ('127.0.0.1', 9000)
        name             = "zwtest"
        handler          = ZWHandler
        max_package_size = 8192 
        ns_addrs         = [('127.0.0.1', 10000), ]
        ns_timeout       = 3000
        ns_report_interval  = 5000

    run(Config)



