# coding: utf-8
import os, sys
import socket
import traceback
import time, random
from zbase.base import logger
import packer

from tornado.concurrent import Future
from tornado import stack_context

logger.install("stdout")
log = logger.log

class NameServer:
    def __init__(self, addr):
        self.addr = addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        self.sock.bind(self.addr)
        self.max_packet_size = 1024
        self.alive_time = 30

        self.data = {}

    def run(self):
        log.info('server started ...')
        while True:
            data, client_addr = self.sock.recvfrom(self.max_packet_size)
            try:
                obj = packer.loads(data)
                retobj  = self.handle(client_addr, obj)
                log.info('func=%s addr=%s:%d data=%s ret=%s', 
                        obj[1], client_addr[0], client_addr[1], 
                        obj, retobj)

                retdata = packer.dumps(retobj)
                self.sock.sendto(retdata, client_addr)
            except:
                log.warning(traceback.format_exc())

    def handle(self, clientaddr, data):
        # request: [seqid, method, name, addr] 
        # response: [seqid, name, addrs]
    
        now  = int(time.time())
        seqid, method, name = data[0], data[1], data[2]
        
        if method == 'q': # query
            parts = name.split('.')
            for p in parts:
                rec = self.data.get(p, [])
                if rec:
                    return [seqid, [ x['addr'] for x in rec if now-x['uptime']<self.alive_time ]]
            return [seqid, []]
        elif method == 'r': # report
            addr = data[3]
            x = self.data.get(name)
            if x:
                for row in x:
                    if row['addr'] == addr:
                        row['uptime'] = now
                        return [seqid]

                x.append({'addr':addr, 'uptime':now})
            else:
                self.data[name] = [{'addr':addr, 'uptime':now}]
            return [seqid]

        else:
            return [seqid]

    def query(self, data):
        pass

    def report(self, data):
        pass

class NameServerPacker:
    def __init__(self, seqid=None):
        self.seqid = seqid
        if not self.seqid:
            self.seqid = random.randint(1, 1000000)

    def pack(self, name, method='q', *args):
        self.seqid += 1
        if method == 'q':
            obj = [self.seqid, method, name]
            return packer.dumps(obj)
        elif method == 'r':
            obj = [self.seqid, method, name, args[0]]
            return packer.dumps(obj)

    def unpack(self, data):
        return packer.loads(data)


class NameServerClient:
    def __init__(self, nameserver_addrs):
        self.nameserver_addrs = nameserver_addrs
        self.nspacker = NameServerPacker()

    def _query(self, nsaddr, name, seqid=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        retobj = None
        tmstart = time.time()
        try: 
            data = self.nspacker.pack(name, 'q')
            n = sock.sendto(data, nsaddr)

            retdata, clientaddr = sock.recvfrom(512)
            retobj = self.nspacker.unpack(retdata)
            log.info('serv=ns func=query addr=%s:%d ret=%s time=%.6f', nsaddr[0], nsaddr[1], retobj, time.time()-tmstart)
            return retobj[1]
        except Exception, e:
            log.info('serv=ns func=query addr=%s:%d ret=%s time=%.6f err=%s', nsaddr[0], nsaddr[1], retobj, time.time()-tmstart, str(e))
            return None
        finally:
            sock.close()

    def _report(self, nsaddr, name, myaddr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        retobj = None
        try: 
            data = self.nspacker.pack(name, 'r', myaddr)
            n = sock.sendto(data, nsaddr)

            retdata, clientaddr = sock.recvfrom(512)
            retobj = self.nspacker.unpack(retdata)
            log.info('serv=ns func=report addr=%s:%d ret=%s', nsaddr[0], nsaddr[1], retobj)
            return 
        except Exception, e:
            log.info('serv=ns func=report addr=%s:%d ret=%s err=%s', nsaddr[0], nsaddr[1], retobj, str(e))
            log.warning(traceback.format_exc())
            return None
        finally:
            sock.close()


    def query(self, name):
        for addr in self.nameserver_addrs:
            ret = self._query(addr, name)
            if ret:
                return ret

    def report(self, server_name, server_addr):
        for nsaddr in self.nameserver_addrs:
            self._report(nsaddr, server_name, server_addr) 


class AsyncNSRequest (object):
    def __init__(self, nsaddrs):
        self.nameserver_addrs = nsaddrs

        self.nspacker = NameServerPacker()
        self.name     = None
        self.method   = 'q'
        self.addr     = None
        
        self.start_time = 0

    def query(self, name):
        self.name   = name
        self.method = 'q'
        return self.nspacker.pack(name)

    def report(self, name, addr):
        self.name   = name
        self.method = 'r'
        self.addr   = addr
        return self.nspacker.pack(name, self.method, addr)

class AsyncNSResponse (object):
    def __init__(self, obj, usetime=0, err=None):
        self.obj = obj
        self.err = err
        self.time = usetime



class AsyncNameServerClient (object):
    def __init__(self, io_loop):
        self._io_loop = io_loop


    def run(self, request, callback, **kwargs):
        future = Future()
        if callback is not None:
            callback = stack_context.wrap(callback)

            def handle_future(future):
                exc = future.exception()
                if exc is not None:
                    response = AsyncNSResponse(None, 0, exc)
                else:
                    response = future.result()
                self.io_loop.add_callback(callback, response)
            future.add_done_callback(handle_future)

        def handle_response(response):
            if response.error:
                future.set_exception(response.error)
            else:
                future.set_result(response)
        self._run(request, handle_response)
        return future

    
    def _run(self, request, callback):
        pass


def main():
    addr = ('127.0.0.1', 10000)
    server = NameServer(addr)
    server.run()

if __name__ == '__main__':
    main()



