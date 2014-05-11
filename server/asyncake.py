# coding: utf-8
import select
import socket
import sys
import time, types
import traceback
import os
from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, ECONNRESET, \
     ENOTCONN, ESHUTDOWN, EINTR, EISCONN, errorcode

from zbase import log
from zbase.server import chain

SELECT = 1
POLL   = 2
EPOLL  = 3
KQUEUE = 4
IOCP   = 5


import threading, Queue

class ThreadPool:
    log = None
    def __init__(self, maxthread=10, maxq=0):
        self.max_thread_size = maxthread
        self.max_queue_size = maxq

        self.queue = Queue.Queue(self.max_queue_size)
        self.threads = [] 
      
        if self.log: self.log.info('init thread:', self.max_thread_size)
        for i in range(0, self.max_thread_size):
            t = threading.Thread(target=self.start, args=())
            t.setDaemon(1)
            t.start()
            self.threads.append(t)

    def start(self):
        while True:
            try:
                task = self.queue.get()
            except:
                continue
            # task: callback:xxx,args:xxxx
            #if self.log: self.log.info('get task:', task)

            if task is None:
                break
    
            try:
                task() 
            except:
                if self.log: self.log.err('task run error:', traceback.format_exc())

    def put(self, task, isblock=True, timeout=0):
        self.queue.put(task, isblock, timeout)



class ExitNow(Exception):
    pass

class IOEvent:
    def __init__(self, sockmap):
        self.socket_map = sockmap

    def register(self, fd, evt):
        pass

    def unregister(self, fd):
        pass

    def modify(self, fd, evt):
        pass

    def close(self):
        pass

    def poll(self, timeout=0):
        pass

    def check_timeout(self, objs=None):
        if objs:
            for obj in objs:
                if obj.timeout():
                    obj.handle_timeout()
        else:
            tm = []
            for fd, obj in self.socket_map.iteritems():
                if isinstance(obj, TimeoutMixin) and obj.timeout():
                    tm.append(obj)

            for obj in tm:
                obj.handle_timeout_event()


class Select (IOEvent):
    core = SELECT

    def read(self, obj):
        try:
            obj.handle_read_event()
        except ExitNow:
            raise
        except:
            obj.handle_error()

    def write(self, obj):
        try:
            obj.handle_write_event()
        except ExitNow:
            raise
        except:
            obj.handle_error()

    def _exception (self, obj):
        try:
            obj.handle_except_event()
        except ExitNow:
            raise
        except:
            obj.handle_error()

    def poll(self, timeout=1):
        if self.socket_map:
            r = []; w = []; e = []
            for fd, obj in self.socket_map.items():
                is_r = obj.readable()
                is_w = obj.writable()
                if is_r:
                    r.append(fd)
                if is_w:
                    w.append(fd)
                if is_r or is_w:
                    e.append(fd)
            if [] == r == w == e:
                time.sleep(timeout)
            else:
                try:
                    r, w, e = select.select(r, w, e, timeout)
                except select.error, err:
                    if err[0] != EINTR:
                        raise
                    else:
                        return

            for fd in r:
                obj = self.socket_map.get(fd)
                if obj is None:
                    continue
                self.read(obj)

            for fd in w:
                obj = self.socket_map.get(fd)
                if obj is None:
                    continue
                self.write(obj)

            for fd in e:
                obj = self.socket_map.get(fd)
                if obj is None:
                    continue
                self._exception(obj)

            self.check_timeout()

class Poll (IOEvent):
    core = POLL 
    def readwrite(self, obj, flags):
        try:
            if flags & (select.POLLIN | select.POLLPRI):
                obj.handle_read_event()
            if flags & select.POLLOUT:
                obj.handle_write_event()
            if flags & (select.POLLERR | select.POLLHUP | select.POLLNVAL):
                obj.handle_except_event()
        except ExitNow:
            raise
        except:
            obj.handle_error()

    def poll(self, timeout=1):
        # Use the poll() support added to the select module in Python 2.0
        if timeout is not None:
            # timeout is in milliseconds
            timeout = int(timeout*1000)
        pollster = select.poll()
        if self.socket_map:
            for fd, obj in self.socket_map.items():
                flags = 0
                if obj.readable():
                    flags |= select.POLLIN | select.POLLPRI
                if obj.writable():
                    flags |= select.POLLOUT
                if flags:
                    # Only check for exceptions if object was either readable
                    # or writable.
                    flags |= select.POLLERR | select.POLLHUP | select.POLLNVAL
                    pollster.register(fd, flags)
            try:
                r = pollster.poll(timeout)
            except select.error, err:
                if err[0] != EINTR:
                    raise
                r = []
            for fd, flags in r:
                obj = self.socket_map.get(fd)
                if obj is None:
                    continue
                self.readwrite(obj, flags)

            self.check_timeout()

class EPoll (IOEvent):
    core = EPOLL
    def __init__(self, sockmap):
        IOEvent.__init__(self, sockmap)
        self._epoll_handler = select.epoll()

    def register(self, fd, evt):
        self._epoll_handler.register(fd, evt)

    def unregister(self, fd):
        self._epoll_handler.unregister(fd)

    def modify(self, fd, evt):
        self._epoll_handler.modify(fd, evt)
    
    def close(self):
        self._epoll_handler.close()
        self._epoll_handler = None

    def _epoll_readwrite(self, obj, flags):
        try:
            if flags & (select.EPOLLIN | select.EPOLLPRI):
                obj.handle_read_event()
            if flags & select.EPOLLOUT:
                obj.handle_write_event()
            if flags & (select.EPOLLERR | select.EPOLLHUP):
                obj.handle_except_event()
        except ExitNow:
            raise
        except:
            obj.handle_error()

    def poll(self, timeout=1.0):
        #timeout -= 1
        if not self._epoll_handler:
            self._epoll_handler = select.epoll()

        if self.socket_map:
            for fd, obj in self.socket_map.items():
                flags = 0
                if obj.readable():
                    flags |= select.EPOLLIN | select.EPOLLPRI
                if obj.writable():
                    flags |= select.EPOLLOUT
                if flags:
                    flags |= select.EPOLLERR | select.EPOLLHUP
                    self._epoll_handler.modify(fd, flags)

            try:
                events = self._epoll_handler.poll(timeout)
            except select.error, err:
                if err[0] != EINTR:
                    raise

            for fileno, event in events:
                obj = self.socket_map.get(fileno)
                if obj is None:
                    self._epoll_handler.unregister(fileno)
                    continue
                self._epoll_readwrite(obj, event)
            self.check_timeout()

class KQueue (IOEvent):
    core = KQUEUE
    def __init__(self, sockmap):
        IOEvent.__init__(self, sockmap)
        self._kq_events = {}
        self._kq_handler = select.kqueue()

    def _kq_readwrite(self, obj, evt):
        try:
            if evt.filter == select.KQ_FILTER_READ:
                obj.handle_read_event()
            if evt.filter == select.KQ_FILTER_WRITE:
                obj.handle_write_event()
            if evt.flags & select.KQ_EV_ERROR:
                obj.handle_except_event()
        except ExitNow:
            raise
        except:
            obj.handle_error()

    def unregister(self, fd):
        evt = self._kq_events.get(fd, 0)
        if evt == 0:
            return

        delevts = []
        if evt & (select.KQ_FILTER_READ * -1): 
            delevts.append(select.kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_DELETE))
        if evt & (select.KQ_FILTER_WRITE * -1):
            delevts.append(select.kevent(fd, filter=select.KQ_FILTER_WRITE, flags=select.KQ_EV_DELETE))
                    
        for ke in delevts:
            self._kq_handler.control([ke], 0)

        #self._kq_events[fd] = 0
        del self._kq_events[fd]


    def register(self, fd, evt):
        addevts = []
        if evt & (select.KQ_FILTER_READ * -1):
            addevts.append(select.kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD))
        if evt & (select.KQ_FILTER_WRITE * -1):
            addevts.append(select.kevent(fd, filter=select.KQ_FILTER_WRITE, flags=select.KQ_EV_ADD))
                    
        for ke in addevts:
            self._kq_handler.control([ke], 0)

        self._kq_events[fd] = evt 

    def modify(self, fd, evt):
        self.unregister(fd)
        self.register(fd, evt)

    def close(self):
        for fd in self._kq_events:
            self.unregister(fd)
        self._kq_events.clear()
        self._kq_handler.close()
        self._kq_handler = None

    def poll(self, timeout=1):
        if not self._kq_handler:
            self._kq_handler = select.kqueue()

        if self.socket_map:
            for fd, obj in self.socket_map.items():
                flags = 0
                if obj.readable():
                    flags |= select.KQ_FILTER_READ * -1
                if obj.writable():
                    flags |= select.KQ_FILTER_WRITE * -1
                   
                oldev = self._kq_events.get(fd)
                if flags:
                    if not oldev:
                        self.register(fd, flags)
                    elif oldev and oldev != flags:
                        self.modify(fd, flags)
                else:
                    if oldev:
                        self.unregister(fd)

            try:
                events = self._kq_handler.control(None, 1000, timeout)
            except select.error, err:
                if err[0] != EINTR:
                    raise

            for event in events:
                fileno = event.ident
                obj = self.socket_map.get(fileno)
                if obj is None:
                    self.unregister(fileno)
                    continue
                self._kq_readwrite(obj, event)
            
            self.check_timeout()

class Iocp (IOEvent):
    pass


class TimeLoop:
    def __init__(self, tm):
        self.tm = tm

    def stop(self):
        self.tm[3] = 0

    def start(self):
        self.tm[3] = 1

    def settime(self, sec=1):
        self.tm[0] = sec

    def wakeup(self):
        self.tm[3] = 1
        self.tm[2] = 1

    def isloop(self):
        return self.tm[1]

    def setloop(self, lp=1):
        self.tm[1] = lp

class AsynCake:
    #log = None
    def __init__(self, coretype=None, tpsize=10):
        self.socket_map = {}

        self._io_map = {SELECT:Select, POLL:Poll, EPOLL:EPoll, 
                        KQUEUE:KQueue, IOCP:Iocp}

        # for timed call. [time, isloop, timestart, status, callchain]
        # isloop: 1 loop, 0 once; status: 1 run, 0 stop
        self._timeevt       = []

        self._threadpool        = None
        self._thread_calls      = []
        self._threadpool_size   = tpsize
        #self._lock = threading.Lock()
        
        self.handler = None
        self.core = coretype
    
        if coretype:
            self.handler = self._io_map[self.core](self.socket_map)
        else:
            if sys.platform.startswith('linux'):
                if hasattr(select, 'epoll'):
                    log.info('use epoll')
                    self.core = EPOLL
                elif hasattr(select, 'poll'):
                    log.info('use poll')
                    self.core = POLL
                else:
                    self.core = SELECT
            elif sys.platform.startswith('win'):
                log.info('use select')
                self.core = SELECT
            elif sys.platform == 'darwin' or sys.platform.startswith('freebsd'):
                log.info('use kqueue')
                self.core = KQUEUE
            else:
                log.info('use select')
                self.core = SELECT
            
            self.handler = self._io_map[self.core](self.socket_map)

    def add_channel(self, channel):
        self.socket_map[channel.fileno] = channel
        channel.map = self
        
        #log.info('add channel:', channel.fileno, channel)

        if self.core == EPOLL:
            self.handler.register(channel.fileno, select.EPOLLIN|select.EPOLLPRI|select.EPOLLERR|select.EPOLLHUP)
        #elif self.core == KQUEUE:
        #    self.handler.register(fileno, select.KQ_FILTER_READ)
            #self._kq_events[fileno] = 0

    def del_channel(self, channel):
        #log.info('del channel:', channel.fileno, channel)

        fileno = channel.fileno
        try:
            del self.socket_map[fileno]
        except:
            return
        else:
            if self.core == EPOLL:
                self.handler.unregister(fileno)
            elif self.core == KQUEUE:
                self.handler.unregister(fileno)

    def loop(self, timeout=30.0, count=None):
        timeout = 0.2
        if count is None:
            while True:
                if not self.socket_map:
                    time.sleep(0.1)

                try:
                    self.handler.poll(timeout)
                except KeyboardInterrupt:
                    raise
                except:
                    log.err('poll error:', traceback.format_exc())
                self._apply_calls()
        else:
            while count > 0:
                if not self.socket_map:
                    time.sleep(0.1)

                try:
                    self.handler.poll(timeout)
                except KeyboardInterrupt:
                    raise
                except:
                    log.err('poll error:', traceback.format_exc())
                count = count - 1

                self._apply_calls()
 
    def close_all(self):
        for fd, obj in self.socket_map.items():
            if self.core == EPOLL:
                self.handler.unregister(fd)
            obj.close()

        if self.core == EPOLL:
            self.handler.close()
        if self.core == KQUEUE:
            self.handler.close()
           
        self.socket_map.clear()


    def _apply_calls(self):
        tnow = int(time.time())
        delx = []
        for i in xrange(0, len(self._timeevt)):
            x = self._timeevt[i]
            #print 'check:', i, tnow, x[:4], tnow-x[2]
            if x[3] > 0 and tnow - x[2] >= x[0]:
                x[2] = tnow
                try:
                    x[4]()
                except:
                    log.err('timed call:', traceback.format_exc())

                if x[1] == 0:
                    delx.insert(0, i)

        for i in delx:
            del self._timeevt[i]
       
        if len(self._thread_calls) > 0:
            while self._thread_calls:
                call = self._thread_calls.pop(0)
                try:
                    call()
                except:
                    log.err('call from thread:', traceback.format_exc())


    def call_later(self, tm, func, *args, **kwargs):
        mycall = chain.CallbackChain()
        mycall.add_callback(func, *args, **kwargs)

        x = [tm, 0, int(time.time()), 1, mycall]
        self._timeevt.append(x)
        return TimeLoop(x)

    def call_chain_later(self, tm, _chain):
        x = [tm, 0, int(time.time()), 1, mycall] 
        self._timeevt.append(x)
        return TimeLoop(x)

    def call_loop(self, tm, func, *args, **kwargs):
        mycall = chain.CallbackChain(False)
        mycall.add_callback(func, *args, **kwargs)
        
        x = [tm, 1, int(time.time()), 1, mycall]
        self._timeevt.append(x)
        return TimeLoop(x)

    def call_chain_loop(self, tm, _chain):
        x = [tm, 1, int(time.time()), 1, mycall]
        self._timeevt.append(x)
        return TimeLoop(x)
    
    def call_stop(self, idx):
        x = self._timeevt[idx]
        x[3] = 0

    def call_resume(self, idx):
        x = self._timeevt[idx]
        x[3] = 1

    def call_remove(self, idx):
        del self._timeevt[idx]

    def call_clear(self):
        self._timeevt = []

    def call_in_thread(self, func, *args, **kwargs):
        if not self._threadpool:
            #ThreadPool.log = self.log
            self._threadpool = ThreadPool(self._threadpool_size) 
        
        mycall = chain.CallbackChain()
        mycall.add_callback(func, *args, **kwargs)
        self._threadpool.put(mycall, False)

    def call_chain_in_thread(self, _chain):
        if not self._threadpool:
            ThreadPool.log = self.log
            self._threadpool = ThreadPool(self._threadpool_size) 
        self._threadpool.put(_chain, False)

 
    def call_from_thread(self, func, *args, **kwargs):
        mycall = chain.CallbackChain()
        mycall.add_callback(func, *args, **kwargs)
       
        self._thread_calls.append(mycall)

    def call_chain_from_thread(self, _chain):
        self._thread_calls.append(_chain)



class Channel:
    debug = False
    connected = False
    accepting = False
    closing = False
    addr = None
    #log = None

    def __init__(self, sock=None, map=None):
        self.map = map
        if sock:
            self.set_socket(sock)
            # I think it should inherit this anyway
            self.socket.setblocking(0)
            self.connected = True
            # XXX Does the constructor require that the socket passed
            # be connected?
            try:
                self.addr = sock.getpeername()
            except socket.error:
                # The addr isn't crucial
                pass
        else:
            self.socket = None

    def __repr__(self):
        status = [self.__class__.__module__+"."+self.__class__.__name__]
        if self.accepting and self.addr:
            status.append('listening')
        elif self.connected:
            status.append('connected')
        if self.addr is not None:
            try:
                status.append('%s:%d' % self.addr)
            except TypeError:
                status.append(repr(self.addr))
        return '<%s at %#x>' % (' '.join(status), id(self))

    def create_socket(self, family, type):
        self.family_and_type = family, type
        self.socket = socket.socket(family, type)
        self.socket.setblocking(0)
        self.fileno = self.socket.fileno()

    def set_socket(self, sock):
        self.socket = sock
        self.fileno = sock.fileno()

        if self.map:
            self.map.add_channel(self)

    def set_reuse_addr(self):
        # try to re-use a server port if possible
        if sys.platform == 'win32':
            reusec = socket.SO_EXCLUSIVEADDRUSE
        else:
            reusec = socket.SO_REUSEADDR
        try:
            self.socket.setsockopt(
                socket.SOL_SOCKET, reusec,
                self.socket.getsockopt(socket.SOL_SOCKET, reusec) | 1)
        except socket.error:
            pass

    def readable(self):
        return True

    def writable(self):
        return True

    # ==================================================
    # socket object methods.
    # ==================================================
    
    def listen_tcp(self, addr, backlog=64):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(addr)
        self.listen(backlog)

    def listen_udp(self, addr=None):
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.set_reuse_addr()
        self.bind(addr)

    def connect_tcp(self, addr):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(addr)

    def listen(self, num):
        self.accepting = True
        if os.name == 'nt' and num > 5:
            num = 1
        return self.socket.listen(num)

    def bind(self, addr):
        self.addr = addr
        return self.socket.bind(addr)

    def connect(self, address):
        self.connected = False
        err = self.socket.connect_ex(address)
        # XXX Should interpret Winsock return values
        if err in (EINPROGRESS, EALREADY, EWOULDBLOCK):
            return
        if err in (0, EISCONN):
            self.addr = address
            self.connected = True
            self.handle_connected()
        else:
            raise socket.error, (err, errorcode[err])

    def accept(self):
        # XXX can return either an address pair or None
        try:
            conn, addr = self.socket.accept()
            return conn, addr
        except socket.error, why:
            if why[0] == EWOULDBLOCK:
                pass
            else:
                raise

    def send(self, data):
        try:
            result = self.socket.send(data)
            return result
        except socket.error, why:
            if why[0] == EWOULDBLOCK:
                return 0
            else:
                raise
            return 0

    def recv(self, buffer_size):
        try:
            data = self.socket.recv(buffer_size)
            if not data:
                # a closed connection is indicated by signaling
                # a read condition, and having recv() return 0.
                self.handle_close()
                return ''
            else:
                return data
        except socket.error, why:
            # winsock sometimes throws ENOTCONN
            if why[0] in [ECONNRESET, ENOTCONN, ESHUTDOWN]:
                self.handle_close()
                return ''
            else:
                raise

    def close(self):
        self.map.del_channel(self)
        self.socket.close()

    # handle internal event

    def handle_read_event(self):
        if self.accepting:
            # for an accepting socket, getting a read implies
            # that we are connected
            if not self.connected:
                self.connected = True
            self.handle_accept()
        elif not self.connected:
            err = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                self.handle_connect_failed()
                self.handle_close()
            else:
                self.handle_connected()
                self.connected = True
                self.handle_read()
        else:
            self.handle_read()

    def handle_write_event(self):
        # getting a write implies that we are connected
        if not self.connected:
            self.handle_connected()
            self.connected = True
        self.handle_write()

    def handle_except_event(self):
        self.handle_except()

    def handle_error(self):
        log.err('uncaptured python exception:\n' + traceback.format_exc())
        if not self.accepting and not self.connected:
            self.handle_connect_failed()
        self.handle_close()

    def handle_except(self):
        log.warn('unhandled exception:', traceback.format_exc())

    def handle_read(self):
        log.warn('unhandled read event')

    def handle_write(self):
        log.warn('unhandled write event')

    def handle_connected(self):
        log.warn('unhandled connect event')

    def handle_connect_failed(self):
        log.warn('unhandled connect failed event')

    def handle_accept(self):
        log.warn('unhandled accept event')

    def handle_close(self):
        self.close()

# Asynchronous File I/O:
#
# After a little research (reading man pages on various unixen, and
# digging through the linux kernel), I've determined that select()
# isn't meant for doing asynchronous file i/o.
# Heartening, though - reading linux/mm/filemap.c shows that linux
# supports asynchronous read-ahead.  So _MOST_ of the time, the data
# will be sitting in memory for us already when we go to read it.
#
# What other OS's (besides NT) support async file i/o?  [VMS?]
#
# Regardless, this is useful for pipes, and stdin/stdout...

if os.name == 'posix':
    import fcntl

    class FileWrapper:
        # here we override just enough to make a file
        # look like a socket for the purposes of asyncore.

        def __init__(self, fd):
            self.fd = fd

        def recv(self, *args):
            return os.read(self.fd, *args)

        def send(self, *args):
            return os.write(self.fd, *args)

        read = recv
        write = send

        def close(self):
            os.close(self.fd)

        def fileno(self):
            return self.fd

    class FileChannel(Channel):

        def __init__(self, fd):
            Channel.__init__(self, None)
            self.connected = True
            self.set_file(fd)
            # set it to non-blocking mode
            flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
            flags = flags | os.O_NONBLOCK
            fcntl.fcntl(fd, fcntl.F_SETFL, flags)

        def set_file(self, fd):
            self._fileno = fd
            self.socket = file_wrapper(fd)
            self.add_channel()

class PipeReadWriteMixin:
    def send(self, data):
        while True:
            try:
                size = os.write(self.writefd, data)
                return size
            except Exception, e:
                if e[0] == EINTR:
                    continue
                if e[0] == EWOULDBLOCK or e[0] == EAGAIN:
                    return 0
                else:
                    raise

    def recv(self, buffer_size):
        while True:
            try:
                s = os.read(self.readfd, buffer_size)
                if not s:
                    self.handle_close()
                    return ''                    
                else:
                    return s
            except Exception, e:
                if e[0] == EINTR:
                    continue

                if e[0] == EWOULDBLOCK or e[0] == EAGAIN:
                    return ''
                else:
                    raise



class PipeReaderChannel (PipeReadWriteMixin, Channel):
    def __init__(self, readfd=None):
        if not readfd:
            self.readfd, self.writefd = os.pipe()
        else:
            self.readfd = readfd
            self.writefd = None

        self.fileno = self.readfd
        self.accepting = False
        self.connected = True

    def close(self):
        os.close(self.readfd)
        if self.writefd:
            os.close(self.writefd)

    def writable(self):
        return False

class PipeWriterChannel (PipeReadWriteMixin, Channel):
    def __init__(self, writefd=None):
        if not writefd:
            self.readfd, self.writefd = os.pipe()
        else:
            self.writefd = readfd
            self.readfd = None

        self.fileno = self.writefd
        self.accepting = False
        self.connected = True

    def close(self):
        os.close(self.writefd)
        if self.readfd:
            os.close(self.readfd)

    def readable(self):
        return False

def pipe_channel():
    r, w = os.pipe()
    return PipeReaderChannel(r), PipeWriterChannel(w)


class TimeoutMixin:
    # timeout time
    _timeout = 0
    # last check timeout
    _time_update = 0
    _timeout_arg = None

    def timeout(self):
        if self._timeout <= 0:
            return False

        tnow = time.time()
        if tnow - self._time_update >= self._timeout:
            return True
        return False
           
    def set_timeout(self, tm=None, arg=None):
        if tm:
            self._timeout = tm
        self._timeout_arg = arg
        self._time_update = time.time()

    def reset_timeout(self):
        self._timeout_update = time.time()

    def clear_timeout(self):
        self._timeout = 0
        self._timeout_arg = None

    def arrive_timeout(self):
        self._time_update = 1

    def handle_timeout_event(self):
        self.handle_timeout(self._timeout_arg)

    def handle_timeout(self, arg=None):
        pass


class UdpServer (Channel):
    def __init__(self, addr, map=None):
        Channel.__init__(self, None, map)
        
        self.listen_udp(addr)

        self.rbuf = ''
        self.wbuf = [] # [(data, addr), ...]
    
    def data_read(self, data, addr):
        pass

    def handle_read(self):
        try:
            data, addr = self.socket.recvfrom(8192)
        except socket.error, e:
            self.handle_error()
        self.data_read(data, addr)

    def buffer_write(self, s, addr):
        self.wbuf.append([s, addr])

    def handle_write(self):
        while len(self.wbuf) > 0:
            # wd: (data, addr)
            wd = self.wbuf.pop(0)
            try:
                n = self.socket.sendto(wd[0], wd[1])
                #if n > 0:
                #    self.wbuf.pop(0)
            except socket.error, e:
                #fixme: continue or break?
                self.handle_error()

    def handle_except(self):
        pass

    def writable(self):
        if len(self.wbuf) > 0:
            return True
        return False
    

class TcpServer (Channel):
    def __init__(self, disp, addr, backlog=64, map=None):
        Channel.__init__(self, None, map)
        self.listen_tcp(addr, backlog)
        self.ChannelClass = disp

    def writable(self):
        return False

    def handle_accept(self):
        conn, addr = self.accept()
        #log.info('accept: ', conn, addr)
        return self.ChannelClass(self, conn, addr, self.map)

    def handle_except(self):
        pass


class TcpChannel (Channel):
    wbufsize = 32768
    rbufsize = 32768

    rbuf = ''
    wbuf = ''

    terminator = '\r\n'
    terminator_func = None

    def __init__(self, server, conn, addr, map):
        Channel.__init__(self, conn, map)
        self.server = server
        self.addr = addr
    
        t = type(self.terminator)
        if type(t) == types.IntType:
            self.terminator_func = self.terminator_int_read
        else:
            self.terminator_func = self.terminator_str_read
    
    def data_read(self, data):
        pass

    def buffer_write(self, data):
        self.wbuf += data

    
    def terminator_int_read(self):
        term = self.terminator
        if len(self.rbuf) < term:
            return
        self.data_read(self.rbuf[:term])
        self.rbuf = self.rbuf[term:]

    def terminator_str_read(self):
        term = self.terminator

        # terminator is string, means must found terminator in data readed
        pos = self.rbuf.find(term)
        if pos < 0:
            return
        pos += len(term)
        self.data_read(self.rbuf[:pos])
        self.rbuf = self.rbuf[pos:]

    def handle_read(self):
        try:
            data = self.recv(self.rbufsize)
        except socket.error, e:
            self.handle_error()
            return
        #if not data: # apply all data before close
        #    self.terminator = 1
        self.rbuf += data
        
        term = self.terminator
        t = type(term)
        while term:
            #print 'terminator:', term, ' rbuf len:', len(self.rbuf)
            # terminator is int, means must read more than terminator length of data
            if t == types.IntType:
                if len(self.rbuf) < term:
                    break
                self.data_read(self.rbuf[:term])
                self.rbuf = self.rbuf[term:]
            elif t == types.StringType or t == types.UnicodeType:
                # terminator is string, means must found terminator in data readed
                pos = self.rbuf.find(term)
                if pos < 0:
                    break
                pos += len(term)
                self.data_read(self.rbuf[:pos])
                self.rbuf = self.rbuf[pos:]

            #log.info(self.rbuf)
        else:
            self.data_read(self.rbuf)
            self.rbuf = ''


    def handle_write(self):
        #log.info('handle write:', len(self.wbuf))
        n = self.send(self.wbuf)
        if n > 0:
            self.wbuf = self.wbuf[n:]
        if not self.wbuf:
            self.data_wrote()

    def data_wrote(self):
        pass
        #log.info('data write complete')

    def writable(self):
        if len(self.wbuf) > 0:
            return True
        return False

    def set_terminator(self, s):
        self.terminator = s

class TcpClient (TcpChannel):
    def __init__(self, addr, map=None):
        TcpChannel.__init__(self, None, None, addr, map)
        self.connect_tcp(addr)

    def handle_connected(self):
        log.info('connected:', self)

    def handle_connect_failed(self):
        log.info('connect failed:', self)

    def handle_except(self):
        pass


def test1():
    log.install('ScreenLogger')

    class TestTcpClient (TcpChannel):
        def data_read(self, data):
            #log.info('read:', data)
            self.buffer_write(data)

    ck  = AsynCake()
    cha = TcpServer(TestTcpClient, ('0.0.0.0', 9000))
    ck.add_channel(cha)

    ck.loop()


def test2():
    #log.install('SimpleLogger')
    log.install('ScreenLogger')
    
    class MyClient (TcpClient):
        def __init__(self, addr):
            TcpClient.__init__(self, addr)
            #self.buffer_write('oh, connected\r\n')

        def data_read(self, data):
            log.info('read:', data)
            self.buffer_write("haha\r\n")

        def handle_except(self):
            log.err('MyClient Exception:', traceback.format_exc())

    class MyServer (TcpServer):
        pass

    class MyChannel (TcpChannel):
        def data_read(self, data):
            self.buffer_write("haha\r\n")

    class MyHttpChannel (TcpChannel):
        terminator = '\r\n\r\n'
        def data_read(self, data):
            #log.info('data_read ...')
            s = 'HTTP/1.1 200\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nhello'
            self.buffer_write(s)

        def data_wrote(self):
            self.close()
        

    log.info('create asyncake!')

    ck  = AsynCake()
    svr = MyServer(MyHttpChannel, ('0.0.0.0', 8000))
    ck.add_channel(svr)
    #cli = MyClient(('0.0.0.0', 10000))
    #ck.add_channel(cli)

    ck.loop()

    log.err('closed')

if __name__ == '__main__':
    test2()


