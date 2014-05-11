# coding: utf-8
import os, sys
import time, datetime
import traceback
import msgpack, struct
import logging

log = logging.getLogger()

VERSION = 1

# {版本: [请求字段，返回字段], ...}
fields = {1: [['ver', 'seqid', 'name', 'data'], ['ver', 'seqid', 'ret', 'data']],
        }

def loads(x):
    #packer = msgpack.Unpacker()
    #packer.feed(x)
    #return packer.unpack()
    return msgpack.unpackb(x)

def loads_response(x):
    obj = loads(x)
    #log.debug('loads: %s', obj)
    ver = obj[0]
    return dict(zip(fields[ver][1], obj))

def dumps(obj):
    #packer = msgpack.Packer()
    #$data = packer.pack(obj)
    #return data
    return msgpack.packb(obj)

def dumps_header(obj):
    s = dumps(obj)
    slen = len(s)
    s = struct.pack('I', slen) + s
    return s

def test():
    print 'test'

if __name__ == '__main__':
    test()


