# coding: utf-8
import string, os, sys, time
import cPickle, shutil, random
import traceback, types
import logging
import uuid, json, base64

log = logging.getLogger()

class SessionError (Exception):
    pass


class SessionStore(object):
    '''http session基类'''
    def __init__(self):
        pass

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, val):
        pass

    def has_key(self, key):
        pass

    def create(self):
        pass
    
    def remove(self, key):
        pass

    def dump(self):
        pass

    def checkall(self, timeout=30):
        pass
    

class DiskSessionStore(SessionStore):
    '''基于磁盘的session'''
    basedir = '/tmp'

    def __init__(self, sid='', args=None):
        self.timeout  = args['expire']
        self.basedir  = args['path']
        self.filepath = ''
        self.sid      = sid
        self.data     = {}

        if not os.path.isdir(self.basedir):
            os.mkdir(self.basedir)
        
        if sid and self.check():
            fpath = os.path.join(self.basedir, sid)
            if os.path.isfile(fpath):
                f = open(fpath, 'rb')
                self.data = cPickle.load(f)
                f.close()
                self.filepath = fpath
            self.data["sid"] = self.sid

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, val):
        self.data[key] = val

    def __str__(self):
        s = '<DiskSessionStore sid:%s path:%s data:%s>' % (self.sid, self.filepath, str(self.data))
        return s
        
    def has_key(self, key):
        return self.data.has_key(key)
        
    def create(self):
        '''创建session'''
        if self.sid:
            return self.sid
        while True:
            self.sid = str(time.time()) + '.' + str(random.randint(1,10000))
            spath = os.path.join(self.basedir, self.sid)
            if os.path.isfile(spath):
                continue
            f = open(spath, 'wb')
            f.close()

            self.filepath = spath
            self.data['sid'] = self.sid
            break
        return self.sid
    
    def keys(self):
        return self.data.keys()

    def get(self, key, defv=None):
        return self.data.get(key, defv)

    def remove(self):
        '''删除session'''
        if self.sid:
            fpath = os.path.join(self.basedir, self.sid)
            if os.path.isfile(fpath):
                os.remove(fpath)
        self.sid  = ''
        self.data = {}
        self.data['sid'] = ''
    
    def dump(self):
        '''session写入文件'''
        if not self.sid:
            self.create()
        fpath = os.path.join(self.basedir, self.sid)
        f = open(fpath, 'wb')
        cPickle.dump(self.data, f)
        f.close()

    def check(self):
        '''检查session文件是否超时,超时删除，没有超时更新访问时间和修改时间'''
        if not self.sid:
            return False
        fpath = os.path.join(self.basedir, self.sid)
        if not os.path.isfile(fpath):
            return False
        sec = self.timeout * 60
        timenow = time.time()
        mtime = os.stat(fpath)[7]
        if timenow - mtime > sec:
            os.remove(fpath)
            return False
        os.utime(fpath, None)
        return True
     

    def checkall(self):
        '''
        检查有超时的session文件就删掉，超时时间timeout单位为分
        '''
        sec = self.timeout * 60
        timenow = time.time()
        files = os.listdir(self.basedir)
        for filename in files:
            filename = os.path.join(self.basedir, filename)
            mtime = os.stat(filename)[7]
            if timenow - mtime > sec:
                os.remove(filename)


memses_data = {}
class MemSessionStore(SessionStore):
    '''存储在内存中的session，不适用于分布式或多进程环境'''
    def __init__(self, sid='', args=None):
        self.timeout  = args['expire']
        self.host     = args['host']
        self.port     = args['port']
        self.sid      = sid
        self.data     = {}
        
        if sid:
            global memses_data
            s, expire = memses_data(self.sid)
            if s:
                self.data = json.loads(s)
            self.data["sid"] = self.sid

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, val):
        self.data[key] = val

    def __str__(self):
        s = '<MemSessionStore sid:%s %s>' % (self.sid, str(self.data))
        return s
        
    def has_key(self, key):
        return self.data.has_key(key)
        
    def create(self):
        '''创建session'''
        self.sid = 'ses%s' % str(uuid.uuid4())
        self.data['sid'] = self.sid
        return self.sid
    
    def keys(self):
        return self.data.keys()

    def get(self, key, defv=None):
        return self.data.get(key, defv)

    def remove(self):
        '''删除session'''
        if self.sid:
            global memses_data
            del memses_data[self.sid]
        self.sid = ''
        self.data = {}
        self.data['sid'] = ''
    
    def dump(self):
        '''session写入文件'''
        if not self.sid:
            self.create()
        v = json.dumps(self.data)
        
        global memses_data
        memses_data[self.sid] = (v, int(time.time())+self.timeout*60)

    def check(self):
        sec = self.timeout * 60
        timenow = time.time()

        global memses_data
        s, expire = memses_data(self.sid)
        if timenow-expire > sec:
            del memses_data[self.sid]
         
    def checkall(self):
        '''
        检查有超时的session文件就删掉，超时时间timeout单位为分
        '''
        sec = self.timeout * 60
        timenow = time.time()
        dels = []
        global memses_data
        for k,v in memses_data.iteritems():
            if timenow - v[1] > sec:
                dels.append(k)
        for k in dels:
            del memses_data[k]



class DBSessionStore(SessionStore):
    '''session数据库存储'''
    def __init__(self, path, sid):
        pass

class MemcachedSessionStore(SessionStore):
    '''session存储在memcache'''
    def __init__(self, path, sid):
        pass

try:
    import redis
except:
    pass
else:
    DATA_NO_CHANGE = 1
    DATA_CHANGED   = 2
    DATA_DELETE    = 3
    class RedisSessionStore (SessionStore):
        '''session存储在redis'''
        def __init__(self, sid='', args=None):
            self.timeout  = args['expire']
            self.host     = args['host']
            self.port     = args['port']
            self.sid      = sid
            self.data     = {}
            self._data_changed = DATA_NO_CHANGE
            
            self.m = redis.Redis(host=self.host, port=self.port, db=0)
            if sid:
                s = self.m.get(self.sid)
                if s:
                    self.data = json.loads(s)
                self.data["sid"] = self.sid

        def __getitem__(self, key):
            return self.data[key]

        def __setitem__(self, key, val):
            self._data_changed = DATA_CHANGED
            self.data[key] = val

        def __str__(self):
            s = '<RedisSessionStore sid:%s %s:%d %s>' % (self.sid, self.host, self.port, str(self.data))
            return s
            
        def has_key(self, key):
            return self.data.has_key(key)
            
        def create(self):
            '''创建session'''
            self.sid = 'ses%s' % base64.b32encode(uuid.uuid4().bytes).strip('=')
            self.data['sid'] = self.sid

            return self.sid
        
        def keys(self):
            return self.data.keys()

        def get(self, key, defv=None):
            return self.data.get(key, defv)

        def remove(self):
            '''删除session'''
            if self.sid:
                self.m.delete(self.sid)
            self.sid = ''
            self.data = {}
            self.data['sid'] = ''
            self._data_changed = DATA_DELETE
        
        def dump(self):
            '''session写入文件'''
            if not self.data:
                return
            if self._data_changed == DATA_NO_CHANGE:
                ret = self.m.expire(self.sid, self.timeout)
                log.debug('redis expire %s:%d ret:%d' % (self.sid, self.timeout, ret))
                return
            if not self.sid:
                self.create()
            v = json.dumps(self.data, separators=(',', ':'))
            if self.timeout > 0:
                ret = self.m.set(self.sid, v, ex=self.timeout)
                #ret = self.m.expire(self.sid, self.timeout)
                log.debug('redis set %s:%s %d ret:%d' % (self.sid, v, self.timeout, ret))
            else:
                ret = self.m.set(self.sid, v)
                log.debug('redis set %s:%s ret:%d' % (self.sid, v, ret))
 

        def check(self):
            pass 

        def checkall(self):
            pass 

class Session:
    def __init__(self, args, sid=None):
        self.store = None
        self.args  = args
        self.session_class =  globals()[args['store']]

        self.start(sid)

    def check(self):
        '''检查session是否存在'''
        if not self.store: # or not self.session.filepath:
            return False
        return True

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, val):
        self.store[key] = val

    def __str__(self):
        p = ''
        for k,v in self.store.data.iteritems():
            p += '%s:%s ' % (str(k), str(v))
        s = '<Session timeout:%d %s>' % (self.timeout, p)
        return s

    def has_key(self, key):
        return self.store.data.has_key(key)

    def get(self, key, defv=None):
        return self.store.data.get(key, defv)

    def start(self, sid):
        '''创建session存储对象'''
        try:
            ses = self.session_class(sid, self.args)
        except:
            log.error(traceback.format_exc())
            raise SessionError, 'not create session store'
        self.store = ses
        return ses
    
    def end(self):
        '''保存session存储对象中的内容到存储'''
        if not self.store:
            return
        if self.store.data:
            self.store.dump()

    def new(self, **kwargs):
        '''写入session数据到存储'''
        self.store.data.update(kwargs)
        self.end()

    def create(self):
        '''创建session id，一般不需要手动调用'''
        return self.store.create()

    def clear(self):
        '''删除session'''
        if not self.store:
            return
        self.store.remove()
        self.store = None

try:
    from Crypto.Cipher import AES  
    from Crypto import Random
except:
    pass
else:
    def aes_enc(data, password):  
	bs = AES.block_size  
	pad = lambda s: s + (bs - len(s) % bs) * chr(bs - len(s) % bs)  
	iv = Random.new().read(bs)  
	cipher = AES.new(password, AES.MODE_CBC, iv)  
	data = cipher.encrypt(pad(data))  
	data = iv + data  
	return data  
      
    def aes_dec(data, password):  
	bs = AES.block_size  
	if len(data) <= bs:  
	    return data  
	unpad = lambda s : s[0:-ord(s[-1])]  
	iv = data[:bs]  
	cipher = AES.new(password, AES.MODE_CBC, iv)  
	data  = unpad(cipher.decrypt(data[bs:]))  
	return data   

    def _json_default_trans(obj):
        '''json对处理不了的格式的处理方法'''
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        raise TypeError('%r is not JSON serializable' % obj)


    class SessionCrypto:
        def __init__(self, key, sid=None):
            self.key = key
            self.sid = sid
            self.data = {}
            
            if sid:
                self._load_data()

        def __getitem__(self, key):
            return self.data[key]

        def __setitem__(self, key, val):
            self.data[key] = val

        def __str__(self):
            p = ''
            for k,v in self.data.iteritems():
                p += '%s:%s ' % (str(k), str(v))
            s = '<SessionCrypto %s>' % (p)
            return s

        def has_key(self, key):
            return self.data.has_key(key)

        def get(self, key, defv=None):
            return self.data.get(key, defv)

        def _load_data(self):
            s = aes_dec(base64.b64decode(self.sid), self.key)
            self.data = json.loads(s)

        def end(self):
            '''创建session id，一般不需要手动调用'''
            jdata = json.dumps(self.data, default=_json_default_trans, separators=(',', ':'))
            self.sid = base64.b64encode(aes_enc(jdata, self.key))
            return self.sid

        def clear(self):
            '''删除session'''
            self.data = {}


# for tornado
def check_login(sescfg):
    def _check(func):
        def _(self, *args, **argitems):
            self.ses = Session(sescfg, self.request.get_cookie('sid', ''))
            if not self.ses.check():
                self.session_error()
                return fail(ERR_SESSION)
            sescheck_func = getattr(self, 'session_check')
            if sescheck_func:
                self.session_error() 
                return fail(ERR_SESSION)
            ret = func(self, *args, **argitems)
            
            se = self.ses.session
            if se.data:
                self.ses.end()
                self.request.set_cookie('sid', se.sid)

            return ret
        return _
    return _check

def rpc_check_login(sescfg, checkfunc):
    def _check(func):
        def _(self, stream, data):
            self.ses = Session(sescfg, data['sid'])
            if not self.ses.check():
                self.session_error()
                return fail(ERR_SESSION)
            if not checkfunc(self.ses):
                self.session_error() 
                return fail(ERR_SESSION)
            ret = func(self, *args, **argitems)
            se = self.ses.session
            if se.data:
                self.ses.end()
                ret['sid'] = data['sid']
            return ret
        return _
    return _check




def test1():
    ds = DiskSessionStore('testses')
    ds['a'] = 'fffff'
    ds[6] = 6666

    ds.close()
    
    s = ds.sid
    print 'sid:', s
    ds = DiskSessionStore('testses', s)
    print ds.keys()
    for k in ds.keys():
        print k, ds[k]

    ds.checkall(1)    
    ds.remove()

def test2():
    key = '1234567812345678'
    x = SessionCrypto(key)
    x['name'] = 'zhaowei'
    x['id'] = 11237

    x.end()
    print x.data
    print x.sid, len(x.sid)
    print '-'*60
    x2 = SessionCrypto(key, x.sid)
    print x2.data




if __name__ == '__main__':
    test2()

