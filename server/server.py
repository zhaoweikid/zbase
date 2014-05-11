# coding: utf-8
import os, sys
import socket, traceback
import errno, threading
import SocketServer
from zbase import sockfile
import logging

log = logging.getLogger()

class TcpServer:
    def __init__(self, addr, handler, threads=1, procs=1):
        self.addr = addr
        self.handler = handler
        self.maxprocs = procs
        self.maxthreads = threads
        
        self.mutex = None
        self.threads = []

        self.create_socket()
        self.create_server()
    
    def create_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.addr)
        self.sock.listen(256)

    def create_server(self):
        if self.maxprocs > 1:
            for i in range(0, self.maxprocs - 1):
                newpid = os.fork()
                if newpid < 0:
                    log.err('fork error!')
                elif newpid == 0:  # child
                    break

        if self.maxthreads > 1:
            self.mutex = threading.Lock()
            for i in range(0, self.maxthreads - 1):
                th = threading.Thread(target=self.loop, args=())
                th.setDaemon(1)
                th.start()
                self.threads.append(th)

    def loop(self):
        while True:
            #if self.mutex:
            #    self.mutex.acquire()

            try:
                newsock, newaddr = self.sock.accept()
            except socket.error, e:
                if e[0] == errno.EAGAIN or e[0] == errno.EINTR:
                    continue
                log.err(traceback.format_exc())
                continue
            finally: 
                #if self.mutex:
                #    self.mutex.release()
                pass
            
            try:
                self.handler(newsock, newaddr, self)
            except:
                log.err(traceback.format_exc())
            newsock.close()



def test():
    #log.install(log.ScreenLogger)
    class MyHandler:
        def __init__(self, request, client_address, server):
            self.request = request
            self.client_address = client_address
            self.server = server
            
            self.file = sockfile.SocketFile(self.request)
            self.handle() 
 
        def handle(self):
            s = 'HTTP/1.1 200\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello'
            #log.info('client:', self.client_address)
            
            #s = self.request.recv(4096)
            while True:
                line = self.file.readline()
                #log.info(line)
                if line == '\r\n':
                    break
            self.request.send(s)

    class MyHandler2 (SocketServer.StreamRequestHandler):
         def handle(self):
            s = 'HTTP/1.1 200\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello'
            #log.info('client:', self.client_address)
            while True:
                line = self.rfile.readline()
                #log.info(repr(line))
                if line == '\r\n':
                    #log.info('go break')
                    break
            #log.info('wfile:', self.wfile)
            self.wfile.write(s)
       
    import BaseHTTPServer
    class MyHandler3 (BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(self):
            s = "hello"

            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', len(s))

            self.end_headers()

            self.wfile.write(s)
 

    svr = TcpServer(('0.0.0.0', 8080), MyHandler, 1, 4)
    svr.loop()

if __name__ == '__main__':
    test()


