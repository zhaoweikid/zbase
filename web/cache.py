# coding: utf-8
import sys, string, os
import traceback
import time, UserDict
import threading
from zbase.base.logger import log

caches = None

class CacheError (Exception):
    pass

class CacheItem:
    '''interface for cache object, must derive from this class'''
    def __init__(self, key, cachetime=60):
        self.cache_time = cachetime
        self.last_time = 0
        self.key = key
        self.data = None

    def update(self, defv=None):
        self.last_time = time.time()
        return self.data

    def get(self):
        if self.data is None:
            self.update()
        return self.data

    def set(self, v):
        self.data = v
        return True

    def __str__(self):
        return str(self.data)


try:
    from zbase.db import dbpool as db
except:
    CacheDBItem = CacheItem
else:
    class CacheDBItem(CacheItem):
        '''simple database cacher'''
        def __init__(self, key, dbname, sql, cachetime=60, **args):
            CacheItem.__init__(self, key, cachetime)
            self.__dict__.update(args) 
            self.dbname = dbname
            self.sql = sql

        def update(self, defv=None):
            conn = db.acquire(self.dbname)
            try:
                result = db.query(conn, self.sql, True)
                self.data = result
            except Exception, e:
                print e
            finally:
                db.release(conn)
        
        def get(self):
            if not data:
                self.update()
            return self.data
        
        def set(self):
            pass


try:
    import cmemcached
except:
    pass
else:
    class MemcachedCacheItem (CacheItem):
        def __init__(self, key, servers=['127.0.0.1:11211'], timeout=30):
            CacheItem.__init__(self, timeout)
            self.key = key
            self.servers = servers
            self.m = cmemcached.Client(self.servers)

        def update(self, devf=None):
            print 'memcached update...'

        def get(self):
            self.data = self.m.get(self.key)
            return self.data

        def set(self, v):
            return self.m.set(self.key, v)

class Cacher(UserDict.UserDict):
    '''对象缓存'''
    def __init__(self):
        UserDict.UserDict.__init__(self)
        self.locker = threading.Lock()
    
    def set_timeout(self, key, timeout=60):
        v = self.data[key] 
        v.last_time = timeout

    def get(self, key):
        ''' get CacheItem from self.data. if timeout call CacheItem's update method.'''
        v = self.data[key] 
        timenow = int(time.time())
        if v.cache_time > 0 and timenow - v.last_time >= v.cache_time:
            self.locker.acquire()
            try:
                if timenow - v.last_time < v.cache_time:
                    return v
                v.data = v.update(v.data)
                v.last_time = timenow
            except Exception, e:
                log.error(traceback.format_exc())
                #del self.data[key]
                #raise CacheError, 'cache error:' + str(e)
                return None
            finally:
                self.locker.release()
        return v

    def get_data(self, key):
        x = self.get(key)
        return x.get()

    def mget_data(self, keys):
        vals = {}
        for k in keys:
            vals[k] = self.get(k).get()
        return vals

    def set_obj(self, obj):
        self.data[obj.key] = obj
        return True

    def set_value(self, key, val, func, tm=60):
        item = CacheItem(key, tm)
        #item.last_time = int(time.time())
        item.update = func
        item.data = val
        return self.set_obj(item)

    def __getitem__(self, key):
        return self.get(key)
    
    def __setitem__(self, obj):
        self.set_obj(val.key, obj)


def install():
    global caches
    caches = Cacher()
    return caches

def setvalue(k, upfunc, timeout=60):
    global caches
    caches.set_value(k, None, upfunc, timeout)

def setobj(v):
    global caches
    return caches.set_obj(v)

def get(k):
    global caches
    return caches.get_data(k)

def mget(keys):
    global caches
    return caches.mget_data(keys)


def test1():
    class MyCacheItem (CacheItem):
        def update(self, defv=None):
            return defv + 1

    c = Cacher()
    x = MyCacheItem(3)
    x.data = 1000
    c['a'] = x

    for i in range(0, 10):
        print 'a:', c['a']
        time.sleep(1)

    def change(defv):
        return defv + 1

    c.set_value('b', 100, change, 3)

    for i in range(0, 10):
        print 'b:', c['b']
        time.sleep(1)

def test2():
    install()
    
    def loader(data):
        print threading.currentThread().getName() + ' up'
        if data is None:
            return 1
        else:
            return data + 1

    caches.set_value('test', None, loader, 1)
    
    def runner():
        while True:
            x = caches.get_data('test')
            print threading.currentThread().getName() + ' ' + str(x)
   
    th = []
    for i in range(0, 10):
        th.append(threading.Thread(target=runner, args=()))

    for t in th:
        t.start()

    for t in th:
        t.join()

def test3():
    install()

    print '====== test3 ======'
    class MyMemCacheItem (MemcachedCacheItem):
        def __init__(self, k):
            MemcachedCacheItem.__init__(self, k)
        
        def update(self, devf=None):
            self.set('aaa')

    item = MyMemCacheItem('haha')

    print 'item set ret:', item.set('111')
    print 'item get ret:', item.get()

    print 'set:', setobj(item)
    print 'get:', get('haha')

def test4():
    m = cmemcached.Client(['127.0.0.1:11211'])
    m.set('haha', '1111')
    print m.get('haha')

            
if __name__ == '__main__':
    test3()


