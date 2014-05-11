# coding: utf-8
import os, sys, time, datetime
import types, random
import threading
from types import ListType, TupleType
import zbase
from zbase.base.logger import log
from zbase.db import pager

dbpool = None

class DBPoolErrorTimeout (Exception):
    pass

class DBPoolBase:
    def acquire(self, name):
        pass
    
    def release(self, name, conn):
        pass


class DBResult:
    def __init__(self, fields, data):
        self.fields = fields
        self.data = data

    def todict(self):
        ret = []
        for item in self.data:
            ret.append(dict(zip(self.fields, item)))
        return ret

    def __iter__(self):
        for row in self.data:
            yield dict(zip(self.fields, row))

    def row(self, i, isdict=True):
        if isdict:
            return dict(zip(self.fields, self.data[i]))
        return self.data[i]

    def __getitem__(self, i):
        return dict(zip(self.fields, self.data[i]))

class DBFunc:
    def __init__(self, data):
        self.value = data

def timeit(func):
    def _(*args, **kwargs):
        starttm = time.time()
        ret = 0
        num = 0
        err = ''
        try:
            retval = func(*args, **kwargs)
            t = type(retval)
            if t == types.ListType:
                num = len(retval)
            elif t == types.DictType:
                num = 1 
            return retval
        except Exception, e:
            err = str(e)
            ret = -1
            raise
        finally:
            endtm = time.time()
            conn = args[0]
            dbcf = conn.param
            log.info('server=mysql name=%s user=%s addr=%s:%d time=%d ret=%s num=%d err=%s sql=%s', 
                conn.name, dbcf['user'], 
                dbcf['host'], dbcf['port'],
                int((endtm-starttm)*1000000), 
                str(ret), num, err, repr(args[1]))

            #log.info('server=mysql name=%s time=%d ret=%s num=%d err=%s sql=%s', 
            #    args[0].name, int((endtm-starttm)*1000000), 
            #    str(ret), num, repr(err), repr(args[1]))
    return _

class DBConnection:
    def __init__(self, param, lasttime, status):
        self.name       = None
        self.param      = param
        self.conn       = None
        self.status     = status
        self.lasttime   = lasttime

        self._db_func   = ('md5(','time()','datetime(','now(')


    def is_available(self):
        if self.status == 0:
            return True
        return False
 
    def useit(self):
        self.status = 1
        self.lasttime = time.time()

    def releaseit(self):
        self.status = 0

    def connect(self):
        pass
    
    def close(self):
        pass

    def alive(self):
        pass

    def cursor(self):
        return self.conn.cursor()
    
    @timeit
    def execute(self, sql, param=None):
        #log.info('exec:%s', sql)
        cur = self.conn.cursor()
        if param:
            ret = cur.execute(sql, param)
        else:
            ret = cur.execute(sql)
        cur.close()
        return ret
 
    @timeit
    def executemany(self, sql, param):
        cur = self.conn.cursor()
        ret = cur.executemany(sql, param)
        cur.close()
        return ret

    @timeit
    def query(self, sql, param=None, isdict=True):
        '''sql查询，返回查询结果'''
        cur = self.conn.cursor()
        if not param:
            cur.execute(sql)
        else:
            cur.execute(sql, param)
        res = cur.fetchall()
        cur.close()
        #log.info('desc:', cur.description)
        if res and isdict:
            ret = []
            xkeys = [ i[0] for i in cur.description]
            for item in res:
                ret.append(dict(zip(xkeys, item)))
            return ret
        if not res:
            return []
        return res
    
    @timeit
    def query_result(self, sql, param=None):
        '''sql查询，返回查询结果'''
        #log.info('query:%s', sql)
        cur = self.conn.cursor()
        if not param:
            cur.execute(sql)
        else:
            cur.execute(sql, param)
        res = cur.fetchall()
        cur.close()

        if not res:
            return None
        xkeys = [ i[0] for i in cur.description]
        return DBResult(xkeys, res)

    @timeit
    def get(self, sql, param=None, isdict=True):
        '''sql查询，只返回一条'''
        cur = self.conn.cursor()
        if not param:
            cur.execute(sql)
        else:
            cur.execute(sql, param)
        res = cur.fetchone()
        cur.close()
        if res and isdict:
            xkeys = [ i[0] for i in cur.description]
            return dict(zip(xkeys, res))
        else:
            return res

    def value2sql(self, v, charset='utf-8'):
        tv = type(v)
        if tv in [types.StringType, types.UnicodeType]:
            if tv == types.UnicodeType:
                v = v.encode(charset)
            if v.startswith(('now()','md5(')):
                return v
            return "'%s'" % self.escape(v)
        elif isinstance(v, datetime.datetime):
            return "'%s'" % str(v)
        elif isinstance(v, DBFunc):
            return v.value
        else:
            if v is None:
                return 'NULL'
            return str(v)

    def dict2sql(self, d, sp=','):
        x = []
        for k,v in d.iteritems():
            x.append('%s=%s' % (k,self.value2sql(v)))
        return sp.join(x)


    def list2sql(self, d, sp=','):
        x = []
        for v in d:
            x.append(self.value2sql(v))
        return sp.join(x)

    def dict2insert(self, d):
        keys = d.keys() 
        vals = []
        for k in keys:
            vals.append('%s' % self.value2sql(d[k]))
        return ','.join(keys), ','.join(vals)

    def list2insert(self, d):
        keys = []
        vals = []
        for one in d:
            keys.append('%s' % self.value2sql(d[0]))
            vals.append('%s' % self.value2sql(d[1]))
        return ','.join(keys), ','.join(vals)

    def any2insert(self, x):
        if type(x) == types.DictType:
            return self.dict2insert(x)
        return self.list2insert(x)

    def insert(self, table, *value_args, **value_kwargs):
        values = {}
        if value_args:
            values.update(value_args[0])
        if value_kwargs:
            values.update(value_kwargs)

        keys, vals = self.dict2insert(values)
        sql = "insert into %s(%s) values (%s)" % (table, keys, vals)
        return self.execute(sql)

    def update(self, table, values, *where_args, **where_kwargs):
        where = {}
        if where_args:
            where.update(where_args[0])
        if where_kwargs:
            where.update(where_kwargs)

        sql = "update %s set %s" % (table, self.dict2sql(values))
        if where:
            sql += " where %s" % self.dict2sql(where,' and ')
        return self.execute(sql)

    def delete(self, table, *args, **kwargs):
        where = {}
        if args:
            where.update(args[0])
        if kwargs:
            where.update(kwargs)

        sql = "delete from %s" % table
        if where:
            sql += " where %s" % self.dict2sql(where, ' and ')
        return self.execute(sql)

    def select(self, table, where=None, fields='*', suffix=None, isdict=True):
        sql = "select %s from %s" % (fields, table)
        if where:
            sql += " where %s" % self.dict2sql(where, ' and ')
        if suffix:
            sql += ' ' + suffix
        return self.query(sql, None, isdict=isdict)

    def select_sql(self, table, where=None, fields='*', suffix=None):
        if type(fields) in (types.ListType, types.TupleType):
            fields = ','.join(fields)
        sql = "select %s from %s" % (fields, table)
        if where:
            sql += " where %s" % self.dict2sql(where, ' and ')
        if suffix:
            sql += ' ' + suffix
        return sql

    def select_page(self, sql, pagecur=1, pagesize=20):
        return pager.db_pager(self.conn, sql, pagecur, pagesize, count_sql=None, maxid=-1)

    def last_insert_id(self):
        pass

    def start(self): # start transaction
        pass

    def commit(self):
        self.conn.commit() 

    def rollback(self):
        self.conn.rollback()

    def escape(self, s):
        return s

      

def with_mysql_reconnect(func):
    def _(self, *args, **argitems):
        import MySQLdb
        trycount = 3
        while True:
            try:
                x = func(self, *args, **argitems)
            except MySQLdb.OperationalError, e:
                if e[0] >= 2000: # client error
                    try:
                        self.conn.close()
                    except:
                        log.warn(traceback.format_exc())
                        self.conn = None
                    self.connect()

                    trycount -= 1
                    if trycount > 0:
                        continue
                raise
            else:
                return x
 
    return _

class MySQLConnection (DBConnection):
    name = "mysql"
    def __init__(self, param, lasttime, status):
        DBConnection.__init__(self, param, lasttime, status)

        self.connect()

    def useit(self):
        self.status = 1
        self.lasttime = time.time()

    def releaseit(self):
        self.status = 0

    def connect(self):
        engine = self.param['engine']
        if engine == 'mysql':
            import MySQLdb
            self.conn = MySQLdb.connect(host = self.param['host'], 
                                        port = self.param['port'],
                                        user = self.param['user'], 
                                        passwd = self.param['passwd'],
                                        db = self.param['db'], 
                                        charset = self.param['charset'],
                                        connect_timeout = self.param.get('timeout', 0),
                                        )

            self.conn.autocommit(1)
            #self.execute('set names utf8')
            #if self.param.get('autocommit',None):
            #    log.note('set autocommit')
            #    self.conn.autocommit(1) 
            #initsqls = self.param.get('init_command')
            #if initsqls:
            #    log.note('init sqls:', initsqls)
            #    cur = self.conn.cursor()
            #    cur.execute(initsqls)
            #    cur.close()
        else:
            raise ValueError, 'engine error:' + engine
        #log.note('mysql connected', self.conn)

    def close(self):
        self.conn.close()
        self.conn = None

    @with_mysql_reconnect
    def alive(self):
        if self.is_available():
            cur = self.conn.cursor()
            cur.execute("show tables;")
            cur.close()
            self.conn.ping()

    @with_mysql_reconnect
    def execute(self, sql, param=None):
        return DBConnection.execute(self, sql, param)

    @with_mysql_reconnect
    def executemany(self, sql, param):
        return DBConnection.executemany(self, sql, param)

    @with_mysql_reconnect
    def query(self, sql, param=None, isdict=True):
        return DBConnection.query(self, sql, param, isdict)

    @with_mysql_reconnect
    def get(self, sql, param=None, isdict=True):
        return DBConnection.get(self, sql, param, isdict)


    def escape(self, s, enc='utf-8'):
        if type(s) == types.UnicodeType:
            s = s.encode(enc)
        ns = self.conn.escape_string(s)
        return unicode(ns, enc)

    def last_insert_id(self):
        ret = self.query('select last_insert_id()', isdict=False)
        #log.debug('conn:%s last insert id:%s', self.conn, str(ret))
        return ret[0][0]

    def start(self):
        sql = "start transaction"
        return self.execute(sql)
                    

class SQLiteConnection (DBConnection):
    name = "sqlite"
    def __init__(self, param, lasttime, status):
        DBConnection.__init__(self, param, lasttime, status)

    def connect(self):
        engine = self.param['engine']
        if engine == 'sqlite':
            import sqlite3
            self.conn = sqlite3.connect(self.param['db'], isolation_level=None)
        else:
            raise ValueError, 'engine error:' + engine

    def useit(self):
        DBConnection.useit(self)
        if not self.conn:
            self.connect()

    def releaseit(self):
        DBConnection.releaseit(self)
        self.conn.close()
        self.conn = None

    def escape(self, s, enc='utf-8'):
        s = s.replace("'", "\\'")
        s = s.replace('"', '\\"')
        return s
 
    def last_insert_id(self):
        ret = self.query('select last_insert_rowid()', isdict=False)
        return ret[0][0]
 
    def start(self):
        sql = "BEGIN"
        return self.conn.execute(sql)
    
         
class DBConnProxy:
    def __init__(self, masterconn, slaveconn):
        #self.name   = ''
        self._master = masterconn
        self._slave  = slaveconn

        self._modify_methods = set(['execute', 'executemany', 'last_insert_id', 'insert', 'update', 'delete'])

    def __getattr__(self, name):
        if name in self._modify_methods:
            return getattr(self._master, name)
        else:
            return getattr(self._slave, name)


       

class DBPool (DBPoolBase):
    def __init__(self, dbcf):
        # one item: [conn, last_get_time, stauts]
        self.dbconn_idle  = []
        self.dbconn_using = []

        self.dbcf   = dbcf
        self.max_conn = 10
        self.min_conn = 1

        if self.dbcf.has_key('conn'):
            self.max_conn = self.dbcf['conn']
 
        self.connection_class = {}
        x = globals()
        for v in x.itervalues():
            if type(v) == types.ClassType and v != DBConnection and issubclass(v, DBConnection):
                self.connection_class[v.name] = v

        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)

        self.open(self.min_conn)

    def synchronize(func):
        def _(self, *args, **argitems):
            self.lock.acquire()
            x = None
            try:
                x = func(self, *args, **argitems)
            finally:
                self.lock.release()
            return x
        return _

    def open(self, n=1):
        param = self.dbcf
        newconns = []
        for i in range(0, n):
            myconn = self.connection_class[param['engine']](param, time.time(), 0)
            myconn.pool = self
            newconns.append(myconn)
        self.dbconn_idle += newconns

    def clear_timeout(self):
        #log.info('try clear timeout conn ...') 
        now = time.time()
        dels = []
        allconn = len(self.dbconn_idle) + len(self.dbconn_using)
        for c in self.dbconn_idle:
            if allconn == 1:
                break
            if now - c.lasttime > 10:
                dels.append(c)
                allconn -= 1

        if len(dels) > 0:
            log.warn('close timeout db conn:%d', len(dels))
        for c in dels:
            c.close()
            self.dbconn_idle.remove(c)

    @synchronize
    def acquire(self, timeout=10000):
        timeout = timeout / 1000.0
        start = time.time()
        while len(self.dbconn_idle) == 0:
            if len(self.dbconn_idle) + len(self.dbconn_using) < self.max_conn:
                self.open()
                continue
            self.cond.wait(timeout)
            if time.time()-start > timeout:
                raise DBPoolErrorTimeout
       
        conn = self.dbconn_idle.pop(0)
        conn.useit()
        self.dbconn_using.append(conn)

        if random.randint(0,100) > 80:
            self.clear_timeout()

        return conn

    @synchronize
    def release(self, conn):
        self.dbconn_using.remove(conn)
        conn.releaseit()
        self.dbconn_idle.insert(0, conn)
        self.cond.notify()
    
    @synchronize
    def alive(self):
        for conn in self.dbconn_idle:
            conn.alive()

    def size(self):
        return len(self.dbconn_idle), len(self.dbconn_using)
   


class RWDBPool:
    def __init__(self, dbcf):
        self.dbcf   = dbcf
        self.name   = ''
        self.policy = dbcf.get('policy', 'round_robin')
        self.master = DBPool(dbcf.get('master', None))
        self.slaves = []
    
        self._slave_current = -1

        for x in dbcf.get('slave', []):
            self.slaves.append(DBPool(x))

    def get_slave(self):
        if self.policy == 'round_robin':
            size = len(self.slaves)
            self._slave_current = (self._slave_current + 1) % size
            return self.slaves[self._slave_current] 
        else:
            raise ValueError, 'policy not support'

    def get_master(self):
        return self.master

    def acquire(self, timeout=10):
        #log.debug('rwdbpool acquire')
        master_conn = None
        slave_conn  = None
        
        try:
            master_conn = self.master.acquire(timeout)
            slave_conn  = self.get_slave().acquire(timeout)

            return DBConnProxy(master_conn, slave_conn)
        except:
            if master_conn:
                master_conn.pool.release(master_conn)
            if slave_conn:
                slave_conn.pool.release(slave_conn)
            raise
            
    def release(self, conn):
        #log.debug('rwdbpool release')
        conn.master.pool.release(conn.master)
        conn.slave.pool.release(conn.slave)


    def size(self):
        ret = {'master':self.master.size(), 'slave':[]}
        for x in self.slaves:
            key = '%s@%s:%d' % (x.dbcf['user'], x.dbcf['host'], x.dbcf['port'])
            ret['slave'].append((key, x.size()))
        return ret
        




def checkalive(name=None):
    global dbpool
    while True:
        if name is None:
            checknames = dbpool.keys()
        else:
            checknames = [name]
        for k in checknames:
            pool = dbpool[k]
            pool.alive()
        time.sleep(300)

def install(cf):
    global dbpool
    dbpool = {}
    
    for name,item in cf.iteritems():
        #item = cf[name]
        log.info('open db %s %s:%d db:%s user:%s conn:%d', 
            name, item.get('host',''), item.get('port',0), 
            item.get('db',''), item.get('user',''), 
            item.get('conn',0))
        #dbp = DBPool(item)
        if item.has_key('master'):
            dbp = RWDBPool(item)
        else:
            dbp = DBPool(item)
        dbpool[name] = dbp
    return dbpool

def acquire(name, timeout=10000):  
    global dbpool
    #log.info("acquire:", name)
    pool = dbpool[name]
    x = pool.acquire(timeout)
    x.name = name
    return x

def release(conn):
    global dbpool
    #log.info("release:", name)
    pool = dbpool[conn.name]
    return pool.release(conn)

def execute(db, sql, param=None):
    return db.execute(sql, param)
 
def executemany(db, sql, param):
    return db.executemany(sql, param)
    
def query(db, sql, param=None, isdict=True):
    return db.query(sql, param, isdict)
   
def with_database(name, errfunc=None, errstr=''):
    def f(func):
        tname_islist = type(name) in (ListType, TupleType)
        def _(self, *args, **argitems):
            if tname_islist:
                self.db = {}
                for nm in name:
                    self.db[nm] = acquire(nm)
            else:
                self.db = acquire(name)
            x = None
            try:
                x = func(self, *args, **argitems)
            except:
                if errfunc:
                    return getattr(self, errfunc)(error=errstr)
                else:
                    raise
            finally:
                if tname_islist:
                    for nm in name:
                        release(self.db[nm])
                else:
                    release(self.db)
                self.db = None
            return x
        return _
    return f

def test():
    import random
    #log.install("SimpleLogger")
    dbcf = {'test1': {'engine': 'sqlite', 'db':'test1.db', 'conn':1}}
    install(dbcf)
    
    sql = "create table if not exists user(id integer primary key, name varchar(32))"
    print 'acquire'
    x = acquire('test1')
    print 'acquire ok'
    x.execute(sql)

    sql1 = "insert into user values (%d, 'zhaowei')" % (random.randint(1, 100));
    x.execute(sql1)

    sql2 = "select * from user"
    ret = x.query(sql2)
    print 'result:', ret

    print 'release'
    release(x)
    print 'release ok'
 
    print '-' * 60  

    class Test2:
        @with_database("test1")
        def test2(self):
            ret = self.db.query("select * from user")
            print ret

    t = Test2()
    t.test2()


def test1():
    DATABASE = {'jack': # connection name, used for getting connection from pool
                {'engine':'mysql',      # db type, eg: mysql, sqlite
                 'db':'jack',       # db table
                 'host':'127.0.0.1', # db host 
                 'port':3306,        # db port
                 'user':'root',      # db user
                 'passwd':'123456',# db password
                 'charset':'utf8',# db charset
                 'conn':20}          # db connections in pool
           }   

    install(DATABASE)

    while True:
        x = random.randint(0, 10)
        print 'x:', x
        conns = []
        for i in range(0, x):
            c = acquire('jack')
            time.sleep(1)
            conns.append(c)
            print dbpool['jack'].size()

        for c in conns:
            release(c)
            time.sleep(1)
            print dbpool['jack'].size()

        time.sleep(1)
        print dbpool['jack'].size()

def test2():
    zbase.base.logger.install('stdout')
    DATABASE = {'jack': # connection name, used for getting connection from pool
                {'engine':'mysql',      # db type, eg: mysql, sqlite
                 'db':'jack',       # db table
                 'host':'127.0.0.1', # db host 
                 'port':3306,        # db port
                 'user':'root',      # db user
                 'passwd':'123456',# db password
                 'charset':'utf8',# db charset
                 'conn':20}          # db connections in pool
           }   

    install(DATABASE)

    x = random.randint(0, 10)
    print 'x:', x
    conns = []
    for i in range(0, x):
        c = acquire('jack')
        time.sleep(1)
        conns.append(c)
        print dbpool['jack'].size()

    for c in conns:
        release(c)
        time.sleep(1)
        print dbpool['jack'].size()

    while True:
        time.sleep(1)
        c = acquire('jack')
        print dbpool['jack'].size()
        release(c)
        print dbpool['jack'].size()

def test3():
    zbase.base.logger.install('stdout')
    DATABASE = {'jack': # connection name, used for getting connection from pool
                {'engine':'sqlite',      # db type, eg: mysql, sqlite
                 'db':'jack.db',       # db table
                 'charset':'utf8',# db charset
                 'conn':20}          # db connections in pool
           }   

    install(DATABASE)

    x = random.randint(0, 10)
    print 'x:', x
    conns = []
    for i in range(0, x):
        c = acquire('jack')
        time.sleep(1)
        conns.append(c)
        print 'size:',dbpool['jack'].size()

    for c in conns:
        release(c)
        time.sleep(1)
        print dbpool['jack'].size()

    while True:
        time.sleep(1)
        c = acquire('jack')
        print dbpool['jack'].size()
        release(c)
        print dbpool['jack'].size()

def test4():
    from zbase.base import logger
    logger.install('stdout')
    global log
    log = logger.log

    DATABASE = {'test':{
                'policy': 'round_robin',
                'default_conn':'auto',
                'master': 
                    {'engine':'mysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'root',
                     'passwd':'123456',
                     'charset':'utf8',
                     'idle_timeout':60,
                     'conn':20},
                'slave':[
                    {'engine':'mysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'zhaowei_r1',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':20},
                    {'engine':'mysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'zhaowei_r2',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':20},
                    ],
                },

           }

    install(DATABASE)

    while True:
        x = random.randint(0, 10)
        print 'x:', x
        conns = []

        print 'acquire ...'
        for i in range(0, x):
            c = acquire('test')
            time.sleep(1)
            c.insert('ztest', {'name':'zhaowei%d'%(i)})
            print c.query('select count(*) from ztest')
            print c.get('select count(*) from ztest')
            conns.append(c)
            print dbpool['test'].size()

                
        print 'release ...'
        for c in conns:
            release(c)
            time.sleep(1)
            print dbpool['test'].size()

        time.sleep(1)
        print '-'*60
        print dbpool['test'].size()
        print '-'*60
        time.sleep(1)


if __name__ == '__main__':
    test4()
    print 'complete!' 
   


