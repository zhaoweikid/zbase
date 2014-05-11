# coding: utf-8
import os, sys
import time, datetime
import socket, struct
import msgpack, random
from zbase.base import logger
import traceback
import packer
from tornado.ioloop import IOLoop, TimeoutError

log = logger.install('stdout')

conns = {}

class RPCCallError (Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg  = msg

class Connection:
    def __init__(self, sock):
        self.sock = sock
        self.uptime = time.time()


def ns_write(sock, fd, events):
    pass

def ns_read(sock, fd, events):
    pass

class AsyncCaller:
    def __init__(self, timeout, obj, myloop=None):
        self.timeout = timeout
        self.obj = obj

        s = packer.dumps(obj)
        self.data = struct.pack('I', len(s)) + s
        
        self.ioloop = myloop
        if not self.ioloop:
            self.ioloop = IOLoop()
        self.conn = None
        self.callback_func = None




    def get(self, timeout=0):
        pass

    def callback(self, func):
        pass

async_runner = None

class Client:
    def __init__(self, nameserver_addr, timeout=1000):
        self.nameserver_addr = nameserver_addr
        self.seqid = random.randint(1, 10000) + id(self)
        self.conn  = None
        self.timeout = timeout
        self.server_addr = None

       
    def call(self, timeout, name, *args, **kwargs):
        if not self.conn:
            ret = self._create_conn(name)
            if not self.conn:
                log.debug('create conn error')
                return None
        xargs = None
        if kwargs:
            xargs = kwargs
        else:
            xargs = args

        self.conn.settimeout(timeout/1000.0)

        self.seqid += 1
        obj = [packer.VERSION, self.seqid, name, xargs]
        s = packer.dumps_header(obj)
        ret = self.conn.send(s)

        while True:
            headstr = self.conn.recv(4)
            if not headstr:
                log.info('read error, closed:%s', repr(headstr))
                return
            headlen = struct.unpack('I', headstr)[0]
            data = self.conn.recv(headlen)
            obj = packer.loads_response(data)

            if obj['seqid'] != self.seqid:
                continue
            if obj['ret'] != 0:
                raise RPCCallError(obj['ret'], obj['data'])
            return obj['data']


    def async_call(self, timeout, name, *args, **kwargs):
        self.seqid += 1
        ac = AsyncResult(timeout, [self.seqid, name, args])
        return ac

    def _create_conn(self, name):
        #ips = self.name2ip(name) 
        ips = nameserver_query(self.nameserver_addr, name)
        if not ips:
            log.info('no server ip')
            return -1

        for ip in ips:
            try:
                self.connect(tuple(ip))
            except:
                log.info(traceback.format_exc())
                continue
            return 0
        log.info('ip %s can not conneced', ips)
        return -1

    def connect(self, addr):
        log.debug('connect to:%s', addr)
        conn = None
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            conn.connect(addr)  
        except:
            log.info('connect error:%s', addr)
            log.info(traceback.format_exc())
            if conn:
                conn.close()
            raise
        self.conn = conn 


def async_thread(ioloop):
    pass 

def test():
    addr = ('127.0.0.1', 10000)
    client = Client(addr)
    client.connect(('127.0.0.1', 9000))
    ret = client.call(1000, 'zwtest.ping', 'haha')
    log.debug('ret:%s', ret)

if __name__ == '__main__':
    test()



