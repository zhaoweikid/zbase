# coding: utf-8
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

HTML_CHARSET = re.compile('<meta.*charset[ =]+([a-zA-Z0-9\-]+).*>')
HEAD_CHARSET = re.compile('charset[ =]+([a-zA-Z0-9\-]+)')

def gzip_uncompress(s):
    buf = cStringIO.StringIO()
    buf.write(s)
    buf.seek(0)
    f = gzip.GzipFile(mode='rb', fileobj=buf) 
    val = f.read()
    f.close()
    return val

def lzw_decompress(compressed):
    # Build the dictionary.
    dict_size = 256
    dictionary = dict((chr(i), chr(i)) for i in xrange(dict_size))
    # in Python 3: dictionary = {chr(i): chr(i) for i in xrange(dict_size)}
    w = result = compressed.pop(0)
    for k in compressed:
        if k in dictionary:
            entry = dictionary[k]
        elif k == dict_size:
            entry = w + w[0]
        else:
            raise ValueError('Bad compressed k: %s' % k)
        result += entry
 
        # Add w+entry[0] to the dictionary.
        dictionary[dict_size] = w + entry[0]
        dict_size += 1
 
        w = entry
    return result

class BaseHTTP:
    read_block_size = 8192
    def __init__(self, usecookie=False):
        self.debuglevel = 0
        if usecookie:
            self.cookie = cookie.ClientCookie()
        else:
            self.cookie = None
        self.clear()

    def clear(self):
        self._headers = {}
        self._method = 'GET'
        self._path = '/'
        self.version = 'HTTP/1.1'
        self._post = {}        
        self._refer = ''
        self._url = ''
        self._realurl = ''
        
        # 接收到的
        self._recv_header = {}
        # 服务器返回的content-length
        self._recv_len = 0
        # 服务器返回的方式是否是chunked
        self._recv_chunked = 0
        self._recv_encoding = ''
        # 服务器返回的协议版本
        self._recv_version = ''
        # 服务器的返回代码
        self._recv_code = 0
        # 服务器返回描述信息
        self._recv_message = ''
        # 是否有跳转到另一个网站，这个相对上一个的
        #self._location_other_site = False
        
        self.host = ''
        self.port = 80
        self.sock = None
        self._file = None
        if self.cookie:
            self.cookie.clear()
        self.timeout = 0
        # 内容语言编码
        self.default_charset = 'gbk'
        self.html_charset = ''
        self.head_charset = ''
        # 是否已经连接上 
        self.isconnected = False
        # 服务器端是否keepalive
        self.keepalive = True

        self.default_header = {
            'User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)',
            'Accept': '*/*', 
            'Accept-Language': 'zh-cn',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'Keep-Alive'
        }
    
    # 清除原来缓冲区的数据
    def clear_data(self):
        pass

    def _connect(self, timeout=30):
        '''最好不要直接调用这个函数，因为有可能host还没有设置'''
        if not self.host:
            raise ValueError, 'host is not assign.'
        for res in socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM):
            log.debug(res)
            af, socktype, proto, canonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
                log.debug("connect: (%s, %s)" % (self.host, self.port))
                self.timeout = timeout
                self.sock.settimeout(timeout)
                self.sock.connect(sa)
            except socket.error, msg:
                log.error('connect fail: (%s, %s)', self.host, self.port)
                if self.sock:
                    self.sock.close()
                self.sock = None
                continue
            self.isconnected = True
            break
        if not self.sock:
            raise socket.error, msg
    
    def settimeout(self, to):
        if self.sock:
            self.timeout = to
            self.sock.settimeout(to)

    def close(self):
        log.warn("http closed")
        if self.sock:
            self.sock.close()
            self.sock = None
            self._file = None
            self.isconnected = False
            self._recv_len = 0
            self.keepalive = True
    
    def put_request(self, method, path):
        '''添加请求类型和请求地址'''
        usp = urlparse.urlparse(path)
        if usp[1]:
            if self.host != usp[1]:
                self.close()
                self.clear()
            self.host = usp[1]
            self.set_header('Host', usp[1])
        newsp = list(usp)
        newsp[0] = ''
        newsp[1] = ''
        
        self._method = method
        self._url = path
        
        self._path = urlparse.urlunparse(newsp)
        if not self._path:
            self._path = '/'
              
    def recv_header(self):
        pass
    
    def parse_recved_header(self, f=None):
        self._recv_header = {}
        self._recv_chunked = 0
        #self._buf = []
        self._post = {}
        
        version = ''
        msgcode = 0
        msg = ''

        isstart = False
        
        log.debug('='*60)

        if not f:
            f = self._recv_header_buf
        f.seek(0)
        while True:
            line = f.readline()            
            if not line:
                break
            line = line.strip()
            log.debug(line)
            if not line:
                continue
            colon = string.find(line, ':')
            
            #fixme: 按照mime的规范，如果一个字段的内容有多行，可以在第二的开头用空格表示是续行
            if colon == -1:  # 没有: 应该是第一行
                msp = line.split(' ')
                version = msp[0]
                self._recv_version = version
                try:
                    msgcode = int(msp[1])
                    self._recv_code = msgcode
                except Exception, e:
                    log.error('get return code error!')
                msg = ' '.join(msp[2:])
                self._recv_message = msg
                isstart = True
            else:
                key = line[:colon].strip()
                val = line[colon+1:].strip()
                
                key = key.lower()
                
                if key == "content-length":
                    try:
                        self._recv_len = int(val)   
                    except Exception, e:
                        log.error('Content-Length error! %s', str(e))
                elif key == 'set-cookie':
                    if self.cookie:
                        self.cookie.add_response(line)
                elif key == 'set-cookie2':
                    if self.cookie:
                        self.cookie.add_response(line)
                elif key == 'transfer-encoding': # chunked说明信息长度不定
                    if string.lower(val) == 'chunked':
                        self._recv_len = -1
                        self._recv_chunked = 1
                elif key == 'content-encoding':
                    self._recv_encoding=val
                elif key == 'connection':
                    if val.lower() == 'close':
                        #self.isconnected = 0
                        self.keepalive = False
                        log.info("note: connection close.")
                elif key == 'content-type':
                    enc = HEAD_CHARSET.search(val)
                    if enc:
                        self.head_charset = string.lower(enc.groups()[0])
                        if self.head_charset == 'gb2312':
                            self.head_charset = 'gbk'
                        log.debug('head charset: %s', self.head_charset)

                self._recv_header[key] = val

        return version, msgcode, msg

    def recv_data(self, filename=''):
        pass

    def recv_response_header(self):
        '''接收服务器发送的头部信息并处理跳转'''
        while 1:
            version, msgcode, msg = self.recv_header()
            log.debug('recv_header return!')
            #if self.debuglevel >1: print self._recv_header
            # 可能有重定向

            ret = self.apply_header()
            if ret:
                self.send_header()
                continue
            else:
                break                
        return version, msgcode, msg


    def apply_header(self, msgcode=None):
        '''处理头部信息里的跳转, 返回False表示不需要处理，返回True表示有跳转'''
        if not msgcode:
            msgcode = self._recv_code
           
        if msgcode >= 300 and msgcode < 400:
            try:
                location = self._recv_header['location']
            except Exception, e:
                log.error('location error! %s', str(e))
            else:
                #self.recv_data()
                refersp = urlparse.urlparse(self._refer)
                urlsp   = urlparse.urlparse(location)
                root    = urlsp[2]
                # 有host
                if urlsp[1]:
                    # 跳转到其他域名了
                    if self.host != urlsp[1]:
                        self.close()
                        self.clear()
                        self.host = urlsp[1]
                    self.set_header('Host', urlsp[1])
                    #self._location_other_site = True
                    # 需要重新连接
                    self.isconnected = False
                if root:
                    if root[0] == '/':
                        urlls = list(urlsp)
                        location = urlparse.urlunparse(urlls)
                    else:
                        urlls = list(urlsp)
                        urlls[2] = os.path.dirname(refersp[2]) + '/' + root
                        location = urlparse.urlunparse(urlls)
                else:
                    location = '/'
                location = unicode(location, self.default_charset)
                #self._refer = ''
                #if self.debuglevel >= 3: print 'location:', location
                # 这里判断可能有问题，或许判断上一次的服务器返回的connection更好
                #-- if self.default_header['Connection'] == 'close':
                #if not self.keepalive:
                    #if self.debuglevel >= 3: loginfo("server not keepalive, close connectioin.")
                    #self.close()
                    #-- if self.debuglevel >= 3: log.info('reconnect ...')
                    #-- self._connect()
                self._realurl = location
                self.put_request('GET', location)
                # 清除前一次请求的数据缓存
                self.clear_data()
               #self.send_header()
                return True
        elif msgcode >= 400 and msgcode < 500:
            self.handle_4xx()
        elif msgcode >= 500:
            self.handle_5xx()
         
        return False

    def recv_response(self):
        '''接收服务器端发送的头部和数据，并自动处理跳转'''
        version, msgcode, msg = self.recv_response_header()
        data = ''
        if self._method in ('GET', 'POST'):
            data = self.recv_data()            
            data = self.apply_data(data) 

        if not self.keepalive:
            log.debug("server not keepalive, close connectioin.")
            self.close()
 

        return version, msgcode, msg, data

    def set_post(self, data):
        '''添加POST数据'''
        if type(data) == types.DictType:
            for k in data:
                v = data[k]
                if type(v) == types.UnicodeType:
                    v = v.encode('gbk')
                self._post[k] = v
            self.set_header('Content-Type',  'application/x-www-form-urlencoded')
            #self.set_header('Content-Length', '')
        else:
            raise TypeError, 'data must be dict'

    def set_headers(self, item):
        '''添加一个header字典到待发送的头部信息里'''
        if type(item) != type({}):
            return
        if not self._headers:
            self._headers = self.default_header.copy()

        self._headers.update(item)
            
    def set_header(self, key, val):
        '''添加一个头部信息'''
        if not self._headers:
            self._headers = self.default_header.copy()
        self._headers[key] = val

    def create_header(self):
        if not self._headers:
            self._headers = self.default_header.copy()
        postdata = ''
        if self._method == 'POST':
            postdata = urllib.urlencode(self._post)
            self.set_header('Content-Length', len(postdata))
        else:
            if self._headers.has_key('Content-Type'):
                del self._headers['Content-Type']
            if self._headers.has_key('Content-Length'):
                del self._headers['Content-Length']
        header = '%s %s %s\r\n' % (self._method, self._path, self.version)
        for k in self._headers:
            v = self._headers[k]
            header = header + '%s: %s\r\n' % (k, v)
        if self._refer:
            header = header + 'Referer: ' + self._refer + '\r\n'
        if self.cookie:
            cookie = self.cookie.client_string() 
            if cookie:
                header = header + cookie + '\r\n'
                    
        header = header + '\r\n'
        header = header + postdata
        log.debug(header)

        if type(header) == types.UnicodeType:
            header = header.encode(self.default_charset)

        return header

    def send_header(self):
        self.clear_data()
        header = self.create_header()
        if not self.isconnected:
            if self.sock:
                self.close()
            self._connect()

        self.sock.send(header)
        #self._refer = self._url
    
    def send_request(self):        
        self.send_header()

    def handle_4xx(self):
        pass
    
    def handle_5xx(self):
        pass
    
    def apply_data(self, data=None):
        head = self._recv_header
        #if not data:
        #    data = ''.join(self._recv_buf)

        if head.has_key('content-encoding'):
            ctenc = head['content-encoding']
            if ctenc == 'gzip':
                data = gzip_uncompress(data)
            elif ctenc == 'deflate':  # deflate应该就是zlib里的压缩算法
                data = zlib.decompress(data, -15)
            elif ctenc == 'compress':
                data = lzw_decompress(data)
        
        contenttype = ''
        if self._recv_header.has_key('content-type'):
            contenttype = self._recv_header['content-type'].lower().strip()
        # only content-type is text/xx use unicode.
        # note: all text/xx are realy html?
        if contenttype.startswith('text'): 
            ret = HTML_CHARSET.search(data)
            if ret:
                self.html_charset = string.lower(ret.groups()[0])
                if self.html_charset == 'gb2312':
                    self.html_charset = 'gbk'
                log.debug('html charset: %s', self.html_charset)
        
            charset = ''
            if self.html_charset:
                charset = self.html_charset
            elif self.head_charset:
                charset = self.head_charset
            else:
                charset = self.default_charset
        
            data = unicode(data, charset, "ignore")
        return data 


    def retr(self, url, postdata=None):
        '''自动下载一个页面的内容,并自动处理编码'''
        if postdata:
            self.put_request('POST', url)
            self.set_post(postdata)
        else:
            self.put_request('GET', url)
            
        self.send_header()

        version, msgcode, msg, data = self.recv_response()
        if msgcode >= 400:
            return None
        return data



class HTTP (BaseHTTP):
    def clear(self):
        BaseHTTP.clear(self)

        # 原来阻塞模式下，读取内容的缓冲区
        self._buf = []
    
    def clear_data(self):
        self._buf = []

    def close(self):
        if self.sock:
            BaseHTTP.close(self)
            self._buf = []
    
    def recv_header_all(self):
        '''接收服务器发送的头部信息'''
        if not self._file:
            self._file = self.sock.makefile('rb', 0)
           
        isstart = False
        rbuf = cStringIO.StringIO()
        while True:
            line = self._file.readline()            
            xlen1 = len(line)
            if xlen1 == 0:
                break
            sline = string.strip(line)
            xlen2 = len(sline)
            #log.debug('%d %s', xlen1, repr(sline))
            if not isstart and sline.startswith('HTTP/'):
                isstart = True
            if isstart and not sline:
                #log.debug('not line, break. orig len:%d strip len:%d', xlen1, xlen2)
                break
            
            rbuf.write(line)

        version, msgcode, msg = self.parse_recved_header(rbuf)
        return version, msgcode, msg
    
    
    def recv_header(self):
        return self.recv_header_all()
    
    def _read_chunked_all(self):
        read_size = 0
        while 1:
            if read_size <= 0:
                line = self._file.readline()
                if not line:
                    return
                #print 'read:', line
                i = line.find(';')
                if i >= 0:
                    line = line[:i]
                # chunked数据的结尾可能是一个空行，这样int肯定会异常
                try:
                    read_size = int(line, 16)
                except:
                    break
            if read_size == 0:
                break
            #print 'chunked data:', read_size
            log.info("chunk size:%d", read_size)
            rsize = self._read_data_all(read_size)
            read_size = read_size - rsize
            # 每一个chunk的最后会有一个\r\n,忽略掉
            if read_size <= 0:
                self._file.read(2)
    
    def _read_data_all(self, size=0):
        rsize = 0
        blocksize = 8192
        if size > 0 and size < blocksize:
            blocksize = size
        while 1:            
            #data = self.sock.recv(blocksize)            
            data = self._file.read(blocksize)
            if not data:
                break
            self._buf.append(data)
            #print data
            rsize = rsize + len(data)            
            if size - rsize < 8192:
                blocksize = size - rsize
            # 这个是有content-length的情况, 和chunked数据两种情况
            if (rsize == size and self._recv_len > 0) or (rsize == size and self._recv_chunked):
                break
        return rsize 


    def recv_data_all(self, filename=''):
        '''接收服务器发送的数据部分'''
        fp = None
        rsize = 0

        if filename:
            fp = open(filename, "wb")            

        if self._recv_chunked == 0:
            self._read_data_all(self._recv_len)
        else:
            self._read_chunked_all()

        if fp:
            for x in self._buf:
                fp.write(x)
            fp.close()
        else:
            return string.join(self._buf, '')
    
    def recv_data(self, filename=''):
        return self.recv_data_all(filename) 

    
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
            log.debug("header _recv_buf count: %d", len(self._recv_buf))
            log.debug("header _recv_buf append: %d", len(ss[end_pos+1:]))
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
            log.debug("header_complete... %d recv_buf: %d", self._recv_header_buf.len, len(self._recv_buf[0]))
            version, msgcode, msg = self.parse_recved_header()
            log.debug(version, msgcode, msg)
            log.debug("recv_buf_len:%d recv_buf:%d", self._recv_buf_len, len(self._recv_buf[0])) 
            log.debug("header:%s", self._recv_header)
            return version, msgcode, msg 
   
    def _check_chunk_string(self):
        s = self._recv_buf[0]
        self._recv_buf = []
        size = len(s)
        pos = 0
        chunkcount = 0
        log.debug('='*20 + '_check_chunk_string' + '='*20)
        log.debug("pos:%d size:%d", pos, size)
        while pos < size: 
            lastpos = pos
            log.debug('lastpos:%d s 10:%s', lastpos, repr(s[pos:pos+10]))
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
                log.debug("chunk string num: %d", num)
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
                    log.debug("chunk head end_pos: %d %s", end_pos, repr(s[end_pos: end_pos+10]))
                    endnum = min(size, end_pos+num)
                    self._recv_buf.append(s[end_pos:endnum])
                    self._recv_buf_len = endnum - end_pos
                    pos = endnum
                    chunkcount += 1
                    log.debug("endnum:%d pos:%d", endnum, pos)
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
        log.debug("chunk head, start_pos: %d", start_pos)
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
        log.debug("laststr:%s, %d", repr(laststr), len(laststr))
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
                log.debug("after header chunk size:%d read size:%d",
                        self._recv_chunk_block_size,  self._recv_buf_len)
            else:
                self._recv_buf_len = 0
        
        log.debug("_recv_chunk_block_size:%d read size:%d", self._recv_chunk_block_size, self._recv_buf_len)
        if self._recv_chunk_block_size <= 0:
            return self._read_chunked_head(self._recv_chunk_header_buf)
        else:
            self._read_chunked_data()
            return False
    
    def read_data_len(self):
        if self._recv_buf_len == -1:
            if self._recv_buf:
                self._recv_buf_len = len(self._recv_buf[0])
                log.debug('check after header data:%d content len:%d', self._recv_buf_len, self._recv_len)
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
            log.debug("send error:%s", str(why[0]))
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
            log.debug("recv error:%s", str(why[0]))
            # winsock sometimes throws ENOTCONN
            if why[0] in [EAGAIN, EINTR, EWOULDBLOCK]:
                log.debug("recv again.")
                return ''
            #elif why[0] in [ECONNRESET, ENOTCONN, ESHUTDOWN]:
                #self.handle_close()
            #    return ''
            else:
                raise


def urlopen(url, post=None, debuglevel=1, usecookie=False):
    h = HTTP(usecookie)
    h.debuglevel = debuglevel
    if post:
        data = h.retr(url, post)
    else:
        data = h.retr(url)

    h.close()
    return data 

def urlopen_try(url, post=None, trycount=1, debuglevel=1):
    while True:
        try:
            data = urlopen(url, post, debuglevel)
        except Exception, e:
            log.warn("open timeout, try next... %s", e)
            trycount -= 1
            if trycount <= 0:
                raise 
            continue
 
        break
    return data

def test(url):
    h = HTTP()
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


