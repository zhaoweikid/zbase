import sys, string, socket, os, re, zlib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cStringIO, StringIO, gzip
import cookie
import urllib, urlparse
from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, ECONNRESET, \
     ENOTCONN, ESHUTDOWN, EINTR, EISCONN, EAGAIN, errorcode
import select, time, types
import httputils
from zbase.base.logger import log
import http


class NoBlockHTTP (BaseHTTP):
    def __init__(self):
        BaseHTTP.__init__(self)
        # 读取header时，最近一次是否最后是换行
        self._recv_header_last_line = False
    
    def clear(self):
        BaseHTTP.clear(self)
        # 有可能接收的buf在接收完头部数据后就已经有数据了
        self._recv_buf = []
        self._recv_buf_len = -1
        # chunk模式下每块chunk的大小
        self._recv_chunk_block_size = -1
        # 接收到的头部数据, 是个StringIO对象
        self._recv_header_buf = None
        # 上次接受的还不够chunk header的数据
        self._recv_chunk_header_buf = ''
    
    def clear_data(self):
        self._recv_header_buf = None
        self._recv_buf = []
        self._recv_buf_len = -1
 

    def close(self):
        if self.sock:
            BaseHTTP.close(self)
            self._recv_buf = []
            self._recv_buf_len = -1
            self._recv_header_buf = None


    def recv_header_one(self):
        '''接收服务器发送的头部信息'''
        if self._recv_header_buf == None:
            self._recv_header_buf = StringIO.StringIO()
        s = self.recv(self.read_block_size)
        #loginfo('header block:', s)        
        log.debug('header block len:%d', len(s))
        lasts = ''
        if self._recv_header_buf.len > 2:
            pos = self._recv_header_buf.tell()
            pos -= 3
            self._recv_header_buf.seek(pos)
            lasts = self._recv_header_buf.read(3)
        log.debug("lasts:%s", lasts)
        start_pos = 0
        end_pos   = 0
        found_header_end = False

        ss = lasts + s
        while True:
            line = ss.find('\n', start_pos)
            if -1 == line:
                break
            ch = ss[line+1]
            if ch == '\r':
                end_pos = line + 2
                break
            elif ch == '\n':
                end_pos = line + 1
                break
            start_pos = line + 1

        log.debug("header end_pos:%d", end_pos) 
        if end_pos > 0:
            self._recv_header_buf.write(ss[len(lasts):end_pos+1])
            log.debug("header _recv_buf count:%d", len(self._recv_buf))
            log.debug("header _recv_buf append:%d", len(ss[end_pos+1:]))
            self._recv_buf.append(ss[end_pos+1:])
            
            # 返回True表示header已经接收完成
            return True
        else:
            self._recv_header_buf.write(s)

        return False
    
    '''这个方法在真正的非阻塞模式下，必须重新定义'''
    def recv_header(self):
        self._recv_header_buf = StringIO.StringIO()

        header_complete = False
        while True:
            header_complete = self.recv_header_one()
            if header_complete:
                break

        if header_complete:
            log.debug("recv buf count:%d", len(self._recv_buf))
            log.debug("header_complete... %d recv_buf:%d", self._recv_header_buf.len, len(self._recv_buf[0]))
            version, msgcode, msg = self.parse_recved_header()
            #log.debug(version, msgcode, msg)
            #log.debug("recv_buf_len: ", self._recv_buf_len, "recv_buf:", len(self._recv_buf[0])) 
            #log.debug("header:", self._recv_header)
            return version, msgcode, msg 
   
    def _check_chunk_string(self):
        s = self._recv_buf[0]
        self._recv_buf = []
        size = len(s)
        pos = 0
        chunkcount = 0
        #log.debug('='*20, '_check_chunk_string', '='*20)
        #log.debug("pos:", pos, "size:", size)
        while pos < size: 
            lastpos = pos
            #log.debug("lastpos: ", lastpos, 's 10:', repr(s[pos:pos+10]))
            while pos < size and s[pos] in '\r\n':
                pos += 1
            if pos >= size: # chunk头部不全
                #return self._read_chunked_head(s[lastpos:])
                log.debug("chunk head not complete.")
                return s[lastpos:]
            end_pos = 0
            checksize = min(size, pos+10) 
            for i in xrange(pos, checksize):
                if s[i] in ';\r\n':
                    end_pos = i
                    break

            if end_pos > 0:
                numstr = s[pos:end_pos]
                log.debug("chunk numstr:%s", numstr)
                num = int(numstr, 16)
                log.debug("chunk string num:%d", num)
                if num == 0:
                    return True
                self._recv_chunk_block_size = num 
            
                if s[end_pos] == ';':
                    end_pos += 1
                
                if end_pos < size and s[end_pos] == '\r':
                    end_pos += 1
                if end_pos < size and s[end_pos] == '\n':
                    end_pos += 1
                 
                # 如果end_pos >= size说明这个chunk head虽然长度取到了，但是不完整
                if end_pos < size:
                    #log.debug("chunk head end_pos:", end_pos, repr(s[end_pos: end_pos+10]))
                    endnum = min(size, end_pos+num)
                    self._recv_buf.append(s[end_pos:endnum])
                    self._recv_buf_len = endnum - end_pos
                    pos = endnum
                    chunkcount += 1
                    #log.debug("endnum:", endnum, "pos:", pos)
                else:
                    #return self._read_chunked_head(s[lastpos:])
                    return s[lastpos:]
            elif chunkcount == 0:
                raise ValueError, "not found chunk size"
        return False

    def _read_chunked_head(self, last=''):
        s = self.recv(10)
        s = last + s
        slen = len(s)
        log.debug("read 10byte:%s", repr(s))

        start_pos = 0
        #if s[0] in '\r\n':
        #    start_pos += 1
        #if s[1] in '\r\n':
        #    start_pos += 1
        #c = s.find(';')
        i = 0
        while i < slen and s[i] in '\r\n':
            i += 1
            start_pos = i
        log.debug("chunk head, start_pos:%d", start_pos)
        end_pos = start_pos
        for i in xrange(start_pos, len(s)):
            if s[i] in ';\r\n':
                end_pos = i
                break
        log.debug("start:%d end:%d", start_pos, end_pos)
        numstr = s[start_pos:end_pos]
        if not numstr:
            self._recv_chunk_header_buf = s
            return False
            
        if numstr[0] == '\\':
            numstr = numstr[2:]
        log.debug("numstr:%s", numstr)
        num = int(numstr, 16)
        if num == 0:
            return True
        log.debug("head num:%d", num)
        self._recv_chunk_block_size = num
        x = s.find('\n', end_pos)
        laststr = s[x+1:]
        log.debug("laststr:%s %d", repr(laststr), len(laststr))
        self._recv_buf.append(laststr)
        self._recv_buf_len = len(laststr)
        return False

    def _read_chunked_data(self):
        rsize = self._recv_chunk_block_size - self._recv_buf_len
        log.debug("try read chunk data size:%d", rsize)
        if 0 == rsize:
            return True
        s = self.recv(rsize)
        if not s:
            return False
        log.debug("read size:%d", len(s))
        self._recv_buf.append(s)
        self._recv_buf_len += len(s)

        if self._recv_buf_len == self._recv_chunk_block_size:
            self._recv_chunk_block_size = 0
            return True
        return False
        
        

    def read_chunked_one(self):
        if self._recv_chunk_block_size == -1:
            if self._recv_buf:
                # 有可能在这里发现所有内容都读取完了
                ret = self._check_chunk_string()
                if type(ret) == types.BooleanType:
                    if ret:
                        return True
                else:
                    self._recv_chunk_header_buf = ret
                    self._recv_chunk_block_size = 0
                log.debug("after header chunk size:%d read size:%d", self._recv_chunk_block_size, self._recv_buf_len)
            else:
                self._recv_buf_len = 0
        
        #log.debug("_recv_chunk_block_size:", self._recv_chunk_block_size, "read size:", self._recv_buf_len)
        if self._recv_chunk_block_size <= 0:
            return self._read_chunked_head(self._recv_chunk_header_buf)
        else:
            self._read_chunked_data()
            return False
    
    def read_data_len(self):
        if self._recv_buf_len == -1:
            if self._recv_buf:
                self._recv_buf_len = len(self._recv_buf[0])
                #log.debug('check after header data:', self._recv_buf_len, ' content len:', self._recv_len)
                if self._recv_buf_len == self._recv_len:
                    return True
            else:
                self._recv_buf_len = 0

        s = self.recv(self.read_block_size)
        if not s: # 这里会有无限循环的读吗
            if self._recv_len == -1:
                return True
            else:
                return False
        self._recv_buf.append(s)
        self._recv_buf_len += len(s)
        
        if self._recv_buf_len == self._recv_len:
            return True
        return False


    '''这个方法在真正的非阻塞模式下，必须重新定义'''
    def recv_data(self, filename=''):
        '''接收服务器发送的数据部分'''
        fp = None
        rsize = 0

        if filename:
            fp = open(filename, "wb")            

        if self._recv_chunked == 0:
            while True:
                if self.read_data_len():
                    break
        else:
            while True:
                if self.read_chunked_one():
                    break

        if fp:
            fp.write(''.join(self._recv_buf))
            fp.close()
        else:
            return ''.join(self._recv_buf)
 
    def send(self, data):
        try:
            result = self.sock.send(data)
            log.debug('send:%d', result)
            return result
        except socket.error, why:
            log.debug("send error:%s", why[0])
            if why[0] in [EWOULDBLOCK, EAGAIN, EINTR]:
                return 0
            else:
                raise
            return 0

    def recv(self, buffer_size):
        try:
            data = self.sock.recv(buffer_size)
            log.debug('read:%d', len(data))
            if not data:
                # a closed connection is indicated by signaling
                # a read condition, and having recv() return 0.
                #self.handle_close()
                return ''
            else:
                return data
        except socket.error, why:
            log.debug("recv error:%s", why[0])
            # winsock sometimes throws ENOTCONN
            if why[0] in [EAGAIN, EINTR, EWOULDBLOCK]:
                log.error("recv again.")
                return ''
            #elif why[0] in [ECONNRESET, ENOTCONN, ESHUTDOWN]:
                #self.handle_close()
            #    return ''
            else:
                raise


                
def testnoblock(url):
    h = NoBlockHTTP()
    h.debuglevel = 2
    h.put_request('GET', url)
    h.send_header()    
    data = h.recv_response()

    print 'http version:', data[0]
    print 'http code:', data[1]
    print 'http message:', data[2]
    print 'http data:\n', data[3].encode(httputils.charset[1])



if __name__ == '__main__':
    install(ScreenLogger)
    #testnoblock(sys.argv[1])
    #test(sys.argv[1])
    #filelog_init()
    url = sys.argv[1]
    print urlopen(url, debuglevel=4)


