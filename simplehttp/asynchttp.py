# coding: utf-8
import sys, string, socket, os, re, zlib, asyncore
import cStringIO, gzip
import cookie
import urllib, urlparse
from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, ECONNRESET, \
     ENOTCONN, ESHUTDOWN, EINTR, EISCONN, errorcode
import select, time, types
from zbase.base.logger import log
from http import NoBlockHTTP

READ_HEAD = 1
READ_DATA = 2
READ_CHUNK= 3

class AsyncHTTP (asyncore.dispatcher):
    def __init__(self, url):
        asyncore.dispatcher.__init__(self)
        
        self.http = NoBlockHTTP()
        
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.http.put_request('GET', url)
        self.http.sock = self.socket 
        s = self.http.create_header()
        #self._read_buffer  = ''
        log.info("request length: ", len(s))
        self._read_flag = False
        self._write_buffer = s

        self._read_status = READ_HEAD

    
    def readable(self):
        #if len(self._read_buffer) > 0:
        log.info("readable:", self._read_flag)
        return self._read_flag

    def writable(self):
        log.info("writeable:", len(self._write_buffer))
        if len(self._write_buffer) > 0:
            return True
        return False
        
    def handle_connect(self):
        self.connect((self.http.host, self.http.port))
        self.http.isconnected = True
        log.info("connect ok!!!")
        


    def handle_read(self):
        log.info("read status: ", self._read_status)
        try:
            if self._read_status == READ_HEAD:
                if self.http.recv_header_one() == True:
                    self.http.parse_recved_header()
                    if self.http.apply_header(): # 有跳转,可以需要重新建立连接
                        head = self.http.create_header() 
                        if not self.http.isconnected:
                            log.info("client is close, reconnect.")
                            if not self.http.sock:
                                log.info("socket is closed, create!")
                                try:
                                    self.close()
                                    self.sock = None
                                except Exception, e:
                                    log.info(e)
                                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
                                self.http.sock = self.socket 
                            self.connected = False
                        self._write_buffer = head
                        self._read_flag = False
                        return
                    
                    if self.http._recv_chunked:
                        self._read_status = READ_CHUNK
                        # 有可能在读头部的时候已经把整个内容都读完了
                        if len(self.http._recv_buf) > 0:
                            if self.http.read_chunked_one():
                                self.handle_complete()
                                self._read_flag = False
                    else:
                        self._read_status = READ_DATA
                        if self.http.read_data_len():
                            self.handle_complete()
                            self._read_flag = False
 
            elif self._read_status == READ_DATA:
                if self.http.read_data_len():
                    self.handle_complete()
                    self._read_flag = False
            elif self._read_status == READ_CHUNK:
                if self.http.read_chunked_one():
                    self.handle_complete()
                    self._read_flag = False
            else:
                log.err("handle_read error! _read_status:", self._read_status)
        except socket.error, why:
            if why[0] in [ECONNRESET, ENOTCONN, ESHUTDOWN]:
                log.err("handle close.")
                self.handle_close()

    def handle_write(self):
        n = self.send(self._write_buffer)
        log.info("send num: ", n, "buffer:", len(self._write_buffer))
        if n == len(self._write_buffer):
            self._read_flag = True
            self._read_status = READ_HEAD
            self._write_buffer = ''
        else:
            self._write_buffer = self._write_buffer[n:]



    def handle_complete(self):
        if not self.http.keepalive:
            self.http.isconnected = False
            self.connected = False

        log.info("complete!!!")
        log.info(self.http.apply_data(''.join(self.http._recv_buf)))
    
    def handle_close(self):
        self.http.close()

    
    def handle_expt(self):
        print 'handle_exception ...'




if __name__ == '__main__':
    h = AsyncHTTP(sys.argv[1])
    
    try:
        asyncore.loop(timeout=10)
    except Exception, e:
        for x in asyncore.socket_map.values():
            log.info('close socket:', x)
            x.close() 
        traceback.print_exc()


