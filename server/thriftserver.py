# coding: utf-8
import os
import sys
import traceback
import multiprocessing
import struct
import logging
import time
import socket
import signal
import thrift
import thrift.protocol

from thrift.Thrift import TException, TMessageType
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from thrift.server.TServer import TServer

import gevent
from gevent.server import StreamServer

log = logging.getLogger()
service = None

class SocketTransport (TTransport.TTransportBase):
    def __init__(self, obj):
        self.socket = obj

    def isOpen(self):
        return True

    def close(self):
        self.socket.close()

    def read(self, sz):
        return self.socket.recv(sz)

    def write(self, buf):
        self.socket.send(buf)

    def flush(self):
        pass

class GTServer(TServer):
    """
    建议不再使用，使用start_gstream
    Gevent socket server based on TServer
    used must after gevent monkey patch
    @yushijun

        handler   = TestHandler()
        processor = Processor(handler)
        transport = TSocket.TServerSocket(host='0.0.0.0', port=8000)
        tfactory  = TTransport.TBufferedTransportFactory()
        pfactory  = TBinaryProtocol.TBinaryProtocolFactory()

        server = GTServer(processor, transport, tfactory, pfactory)
        server.serve()
    """

    def __init__(self, *args):
        TServer.__init__(self, *args)
        self._stop_flag = False

    def stop(self):
        '''stop the server'''
        self._stop_flag = True
        self.serverTransport.close()
        log.debug("server going to stop")

    def serve(self):
        self.serverTransport.listen()
        while not self._stop_flag:
            try:
                client = self.serverTransport.accept()
                gevent.spawn(self._process_socket, client)
            except KeyboardInterrupt:
                log.debug('KeyboardInterrupt')
                self.stop()
            except:
                log.error(traceback.format_exc())

        # wait all greenlet
        gevent.wait()

    def _process_socket(self, client):
        """A greenlet for handling a single client."""
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except:
            log.error(traceback.format_exc())

        itrans.close()
        otrans.close()


class GStreamServer(StreamServer):
    """
    thrift server based on gevent StreamServer
    used must after gevent monkey patch
    @yushijun

        handler   = TestHandler()
        processor = Processor(handler)
        tfactory  = TTransport.TBufferedTransportFactory()
        pfactory  = TBinaryProtocol.TBinaryProtocolFactory()

        server = GStreamServer(('',8000), processor = processor, inputTransportFactory = tfactory, inputProtocolFactory = pfactory)
        server.serve_forever()
    """
    def __init__(self, listener, processor,
                 inputTransportFactory=None, outputTransportFactory=None,
                 inputProtocolFactory=None, outputProtocolFactory=None,
                 backlog=None, spawn='default',  **kwargs):
        StreamServer.__init__(self, listener=listener, handle=self._process_socket, backlog = backlog,
                              spawn = spawn, **kwargs)

        self.processor = processor
        self.inputTransportFactory = inputTransportFactory
        self.outputTransportFactory = outputTransportFactory or inputTransportFactory
        self.inputProtocolFactory = inputProtocolFactory
        self.outputProtocolFactory = outputProtocolFactory or inputProtocolFactory

    def _process_socket(self, client, address):
        """A greenlet for handling a single client."""

        log.info('func=open|client=%s:%d|pool_size=%d', address[0], address[1], len(self.pool))
        client = SocketTransport(client)

        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except EOFError:
            pass
        except:
            log.error(traceback.format_exc())

        itrans.close()
        otrans.close()
        log.info('func=close|client=%s:%d', address[0], address[1])


def handle(client, addr):
    fd = client.fileno()
    log.info('func=open|client=%s:%d', addr[0], addr[1])
    global service
    if not service:
        raise TException('service not initial')

    def read_frame(trans):
        frame_header = trans.readAll(4)
        sz, = struct.unpack('!i', frame_header)
        if sz < 0:
            raise TException('client must use TFramedTransport')
        frame_data = trans.readAll(sz)
        return frame_data

    def unpack_name(s):
        sz, = struct.unpack('!i', s[4:8])
        return s[8:8+sz]

    tstart = time.time()
    trans = SocketTransport(client)
    try:
        #frame_data = read_frame(trans)
        #log.debug('data:%s %s', repr(frame_data), unpack_name(frame_data))
        #itran = TTransport.TMemoryBuffer(frame_data)

        itran = TTransport.TFramedTransport(trans)
        otran = TTransport.TFramedTransport(trans)
        iprot = TBinaryProtocol.TBinaryProtocol(itran, False, True)
        oprot = TBinaryProtocol.TBinaryProtocol(otran, False, True)

        service.handler.remote = addr
        p = service.Processor(service.handler)
        while True:
            p.process(iprot, oprot)
            #log.info('func=call|name=%s|time=%d', unpack_name(frame_data), (time.time()-tstart)*1000000)

        #itran.close()
        #otran.close()
    except TTransport.TTransportException as tx:
        log.error(traceback.format_exc())
        pass
    except EOFError:
        #log.error(traceback.format_exc())
        #log.info('func=close|time=%d', addr[0], addr[1], (timt.time()-tstart)*1000)
        pass
    except Exception as e:
        log.error(traceback.format_exc())
    finally:
        log.info('func=close|time=%d', (time.time()-tstart)*1000000)
        client.close()

def start_gstream(module, handler_class, addr, max_conn=1000, framed=False, max_process = 1, stop_callback = None):
    global service

    handler   = handler_class()
    processor = module.Processor(handler)
    if framed:
        tfactory = TTransport.TFramedTransportFactory()
    else:
        tfactory  = TTransport.TBufferedTransportFactory()
    pfactory  = TBinaryProtocol.TBinaryProtocolFactory()


    def signal_master_handler(signum, frame):
        global service
        log.info("signal %d catched, server will exit after all request handled", signum)
        for i in service:
            i.terminate()
    def signal_worker_handler(signum, frame):
        global service
        log.info("worker [%d] will exit after all request handled", os.getpid())
        service.close()
        if stop_callback:
            stop_callback()

    def server_forever(listener):
        global service
        log.info('worker [%d] start',os.getpid())
        service = GStreamServer( listener, processor = processor, inputTransportFactory = tfactory, inputProtocolFactory = pfactory, spawn=max_conn)
        signal.signal(signal.SIGTERM, signal_worker_handler)
        service.start()
        gevent.wait()
        log.info('worker [%d] exit', os.getpid())

    listener = GStreamServer.get_listener(addr, family = socket.AF_INET)

    log.info('server start at %s:%d pid:%d', addr[0], addr[1], os.getpid())
    if max_process == 1:
        server_forever(listener)
    else:
        service = [multiprocessing.Process(target=server_forever, args=(listener,)) for i in range(max_process)]
        for i in service:
            i.start()
        signal.signal(signal.SIGTERM, signal_master_handler)
        for i in service:
            i.join()


#def start_gevent(module, handler_class, addr, proc_process, max_conn=1000, max_process=1):
def start_gevent(module, handler_class, my_process, addr, max_conn=1000, max_process=1):
    from gevent.pool import Pool
    from gevent.server import StreamServer
    from gevent.socket import wait_write, socket


    module.handler = handler_class()
    global service
    service = module

    pool = Pool(max_conn)

    server = StreamServer(addr, handle, spawn=pool)
    server.reuse_addr = 1
    server.start()

    def server_start():
        # do_trans_all_logger()
        log.info('server started addr=%s:%d pid=%d', addr[0], addr[1], os.getpid())
        server.serve_forever()

    def _start_process(index):
        server_name = 'process%02d' % index
        process = multiprocessing.Process(target=server_start, name=server_name)
        process.start()

        return process

    # 创建工作进程
    processes = [
        _start_process(index)
        for index in range(0, max_process)
    ]
    for item in processes:
        my_process.append(item)
    #proc_process = processes
    # 等待所有的子进程结束
    map(
        lambda p: p.join(),
        processes
    )


def start_threadpool(module, handler_class, addr, max_thread=1, max_proc=1):
    import threadpool, multiprocessing, threading
    from threadpool import ThreadPool, Task
    import socket

    module.handler = handler_class()
    global service
    service = module


    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.bind(addr)
    sock.listen(1024)


    def thread_run():
        # do_trans_all_logger()

        def run(obj, client, addr):
            return handle(client, addr)

        t = ThreadPool(max_thread)
        t.start()

        while True:
            try:
                client, addr = sock.accept()
                t.add(Task(run, client=client, addr=addr))
                print t.queue.qsize()
            except KeyboardInterrupt:
                os.kill(os.getpid(), 9)
            #except Queue.Full:
            #    client.close()
            except:
                client.close()
                log.debug(traceback.format_exc())

    def _start_process(index):
        server_name = 'process%02d' % index
        process = multiprocessing.Process(target=thread_run, name=server_name)
        process.start()

        return process

    # 创建工作进程
    processes = [
        _start_process(index)
        for index in range(0, max_proc)
    ]

    # 等待所有的子进程结束
    map(
        lambda p: p.join(),
        processes
    )

class RunGeventServer:
    def __init__(self, module, handler_class, addr, max_conn=1000, max_process=1):
        self.my_process = []
        self.module = module
        self.handler_class = handler_class
        self.addr = addr
        self.max_conn = max_conn
        self.max_process = max_process


    def run(self):
        start_gevent(self.module, self.handler_class, self.my_process, self.addr, self.max_conn, self.max_process)

    def stop(self):
        for p in self.my_process:
            p.terminate()

def trans_logger(logger):
    """
    替换传入的logger中的所有file_handler的stream。
    替换方法为： 将stream使用的文件如filename, 更改为 filename.进程名
    目的： 避免多个进程写同一个文件导致错误，已知错误有：切日志异常、日志会丢失、日志会混乱等
    :param logging.Logger logger:
    :return:
    """

    if not isinstance(logger, logging.Logger):
        return

    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.baseFilename = handler.baseFilename + '.' + multiprocessing.current_process().name

            old_stream = handler.stream
            if old_stream:
                try:
                    old_stream.flush()
                finally:
                    if hasattr(old_stream, "close"):
                        old_stream.close()

                handler.stream = handler._open()

def do_trans_all_logger():
    """
    转换所有的logger，包括root-logger 和 其它logger
    :return:
    """

    r = logging.Logger.root
    m = logging.Logger.manager

    # 根logger
    trans_logger(r)
    # 其它logger
    for logger in m.loggerDict.items():
        trans_logger(logger)

def test():
    pass

if __name__ == '__main__':
    test()


