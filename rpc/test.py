# coding: utf-8
import os, sys
import nameserver
import tornado
import tornado.ioloop
import tornado.httpclient
import time
from zbase.base import logger
log = logger.install('stdout')

def test():
    addr = [('127.0.0.1', 10000)]
    client = nameserver.NameServerClient(addr)
    client.report('zwtest', ('127.0.0.1', 9000))
    client.query('zwtest')


def test_http():
    h = tornado.httpclient.HTTPClient()
    ret = h.fetch('http://www.baidu.com')
    log.debug('type:%s dir:%s', type(ret), dir(ret))

def test_http_async():
    def handle_result(ret):
        log.debug('result:%s %s', type(ret), dir(ret))
        log.debug('code:%s body:%s', ret.code, len(ret.body))

    h = tornado.httpclient.AsyncHTTPClient()
    ret = h.fetch('http://www.baidu.com', handle_result)
    log.debug('type:%s dir:%s', type(ret), dir(ret))

    log.debug('done:%s', ret.done())
    

    tornado.ioloop.IOLoop.current().start()


test_http_async()
