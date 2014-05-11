# coding: utf-8
import os, sys
import pprint, time
import datetime
import types, string, re
import traceback, logging
from zbase.base.logger import log
from zbase.db import dbpool, pager
from zbase.db.fields import *


class TableMeta (type):
    def __new__(cls, name, bases, dct):
        for k,v in dct.iteritems():
            if not k.startswith('__') and type(v) == types.FunctionType:
                v = classmethod(v)
                dct[k] = v
            elif isinstance(v, Column):
                v.table = dct['_tablename']
                v.name  = k
                if v.show:
                    v.error = '<'+v.show+'>' + u'错误'
                else:
                    v.error = k + ' error'
        return type.__new__(cls, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super(TableMeta, cls).__init__(name, bases, dct)  

class TableModel (object):
    __metaclass__ = TableMeta

    def sql(self):
        keys = []
        lines = []
        lines.append('CREATE TABLE %s (' % self._tablename)
        #print self.__dict__
        #for k,v in self.__class__.__dict__.iteritems():
        for k,v in self.__dict__.iteritems():
            #print k, v, self.__class__.__name__
            if isinstance(v, Column):
                # name, type, maxlen, isnull, default, key, autoinc
                x = []
                x.append(v.name)
                if v.type.maxlen > 0:
                    x.append(v.type.typestr+'(%d)' % v.type.maxlen)
                else:
                    x.append(v.type.typestr)
                if not v.isnull:
                    x.append('not null') 
                if v.type.default:
                    x.append("default %s" % Value(v.type.default))
                if v.primary_key:
                    x.append("primary key")
                elif v.unique:
                    x.append("unique")

                if v.autoinc:
                    x.append("auto_increment")
                if v.key:
                    keys.append(v.name)
                
                if v.name == 'id':
                    lines.insert(1, "\t%s," % (' '.join(x)))
                else:
                    lines.append("\t%s," % (' '.join(x)))
        if keys:
            lines.append("\tKEY (%s)" % (','.join(keys)))
        else:
            lines[-1] = lines[-1][:-1]
        lines.append(');')
        return '\n'.join(lines) + '\n'
    
    def new(self):
        return globals()['TableRow'](self)
       
    def objects(self):
        return globals()['TableQuery'](self, None)

    def db(self, conn=None):
        return globals()['TableQuery'](self, conn)

    def names(self):
        '''字段名列表'''
        x = []
        for k,v in self.__dict__.iteritems():
            if not k.startswith('_'):
                x.append(k)
        return x

    def fields(self, fs=None):
        '''获取指定字段名的字段字典'''
        x = {}
        for k,v in self.__dict__.iteritems():
            if not k.startswith('_'):
                if not fs:
                    x[k] = v
                    continue
                if k in fs:
                    x[k] = v
        return x

    def fields_list(self, fs=None):
        x = []
        for k,v in self.__dict__.iteritems():
            if not k.startswith('_'):
                if not fs:
                    x.append(v)
                    continue
                if k in fs:
                    x.append(v)
        return x


    def fields_filter_attr(self, val):
        '''获取指定属性的字段'''
        x = {}
        for k,v in self.__dict__.iteritems():
            if not k.startswith('_'):
                if v.attr & val:
                    x[k] = v
        return x

    def choice(self, keys=None):
        x = {}
        for k,v in self.__dict__.iteritems():
            if not k.startswith('_'):
                if keys:
                    if k in keys:
                        x[k] = v.choice   
                else:
                    x[k] = v.choice   
        return x
                
def Value(v, conn=None, charset='utf-8'):
    tv = type(v)
    if tv in [types.StringType, types.UnicodeType]:
        if tv == types.UnicodeType:
            v = v.encode(charset)
        if v.startswith(('now()','md5(')):
            return v
        if conn:
            return "'%s'" % conn.escape(v)
        return "'%s'" % v
    elif isinstance(v, datetime.datetime):
        return "'%s'" % str(v)
    else:
        if v is None:
            return 'NULL'
        return str(v)


def And(*args, **kwargs):
    if args:
        return '(%s)' % (' and '.join(args))
    elif kwargs:
        x = []
        for k,v in kwargs.iteritems():
            x.append("%s=%s" % (k, Value(v))) 
        return '(%s)' % (' and '.join(x))


def Or(*args, **kwargs):
    if args:
        return '(%s)' % (' or '.join(args))
    elif kwargs:
        x = []
        for k,v in kwargs.iteritems():
            x.append("%s=%s" % (k, Value(v))) 
        return '(%s)' % (' or '.join(x))

def Great(left, right):
    return '%s>%s' % (left, Value(right))

def GreatEqual(left, right):
    return '%s>=%s' % (left, Value(right))

def Less(left, right):
    return '%s<%s' % (left, Value(right))

def LessEqual(left, right):
    return '%s<=%s' % (left, Value(right))

def NotEqual(left, right):
    return '%s<>%s' % (left, Value(right))

def Equal(left, right):
    return '%s=%s' % (left, Value(right))

def Between(left, right1, right2):
    return '%s between %s and %s' % (left, Value(right1), Value(right2))

def IsNull(left):
    return 'isnull(%s)' % left

def Min(left):
    return 'min(%s)' % left

def Max(left):
    return 'max(%s)' % left

def In(field, *args):
    x = [Value(k) for k in args]
    return '%s in(%s)' % (field, ','.join(x))


def Condition(tmodel, data, conn=None, charset='utf-8'):
    if not isinstance(data, InputItem): 
        data = data2inputitem(data)

    x = []
    for k,v in data.iteritems():
        x.append('%s%s%s' % (k, v.op, Value(v.value(), conn, charset)))
    return ' and '.join(x)


def _conn_selector(func):
    def _x(self, **kwargs):
        #if type(self.conn) in self._strtype:
        if not self._conn:
            log.warn('_conn_selector create new conn')
            self._conn = dbpool.acquire(self.model._dbname)
            try:
                return func(self, **kwargs) 
            finally:
                dbpool.release(self._conn)
                self._conn = None
        else:
            return func(self, **kwargs) 
    return _x


class Table:
    def __init__(self, tmodel, name=None):
        self.mode = tmodel
        self.name = name
        if not self.name:
            self.name = model._tablename

class TableQuery:
    def __init__(self, model, conn=None, tablename=None):
        self._listtype = [types.ListType, types.TupleType]
        self._strtype  = [types.StringType, types.UnicodeType]
        
        self.model = model
        self._tablename = tablename
        if not self._tablename:
            self._tablename = self.model._tablename
        self._conn_create = False
        if conn:
            self._conn = conn
        else:
            self._conn  = dbpool.acquire(self.model._dbname)
            self._conn_create = True
        self._sql  = ''
        self.charset = 'utf-8'

    def __del__(self):
        if self._conn_create:
            log.note('release:%s', self._conn.conn)
            dbpool.release(self._conn)

    def clear(self):
        self._sql = ''

    def get(self, _cond=None, **kwargs):
        self._sql = "select * from %s" % (self._tablename)
        self.where(_cond, **kwargs)
        rets = self.query_list()
        if not rets:
            return None
            #raise ValueError, 0
        #if len(rets) != 1:
        #    raise ValueError, len(rets)
        return TableRow(self.model, rets[0], self._conn)

    def count(self, _cond=None, **kwargs):
        self._sql = "select count(*) from %s" % (self._tablename)
        self.where(_cond, **kwargs)
        rets = self.query_list(isdict=False)
        return rets[0][0]
 
    def filter(self, _cond=None, **kwargs):
        self._sql = "select * from %s" % (self._tablename)
        if _cond or kwargs:
            self.where(_cond, **kwargs)
        return self.query()

    def sql(self, sql):
        self._sql = sql
        return self.query()

    def select(self, _fields='*', **kwargs):
        tf = type(_fields)
        if tf in self._listtype:
            self._sql = "select %s from %s" % (','.join(_fields), self._tablename)
        elif tf in self._strtype:
            self._sql = "select %s from %s" % (_fields, self._tablename)
        else:
            raise ValueError, _fields
        if kwargs:
            self.where(None, **kwargs)
        return self

    def where(self, _cond=None, **kwargs):
        if _cond:
            self._sql += " where %s" % _cond
        elif kwargs:
            p = []
            for k,v in kwargs.iteritems():
                if isinstance(v, InputItem):
                    p.append("%s%s%s" % (v.k, v.op, self._value(v.v))) 
                else:
                    p.append("%s=%s" % (k, self._value(v))) 
            self._sql += " where %s" % ' and '.join(p)
        return self

    def order_by(self, field):
        self._sql += " order by %s" % field
        return self

    def order_by_desc(self, field):
        self._sql += " order by %s desc" % field
        return self

    def update(self, _set=None, **kwargs):
        self._sql = "update %s set" % self._tablename
        if _set:
            self.set(_set)
        elif kwargs:
            self.set(_set, **kwargs)
        return self

    def set(self, _set=None, **kwargs):
        #log.info('_set:', _set)
        if _set:
            ts = type(_set)
            if ts in self._strtype:
                self._sql += ' ' + _set
            elif ts == types.DictType:
                p = []
                for k,v in _set.iteritems():
                    if isinstance(v, InputItem):
                        p.append(self._pair(v.k,v.v))
                    else:
                        p.append(self._pair(k,v))
                self._sql += " " + ",".join(p)
            elif ts in self._listtype:
                p = []
                for row in _set:
                    if isinstance(v, InputItem):
                        p.append(self._pair(v.k,v.v))
                    else:
                        p.append(self._pair(row[0], row[1]))    
                self._sql += " " + ",".join(p)
        else:
            p = []
            #log.info('kwargs:', kwargs)
            for k,v in kwargs.iteritems():
                #log.info('set k:', k, 'v:', v)
                if isinstance(v, InputItem):
                    p.append(self._pair(v.k,v.v))
                else:
                    p.append(self._pair(k,v))
            self._sql += " " + ",".join(p)
        return self

    def delete(self, _cond=None, **kwargs):
        self._sql = "delete from %s" % self._tablename
        self.where(_cond, **kwargs) 
        return self

    def truncate(self):
        self._sql = "truncate table %s" % self._tablename
        return self

    def insert(self, _fields=None, _values=None, **kwargs):
        if _fields:
            if type(_fields) in self._listtype:
                self._sql = "insert into %s(%s)" % (self._tablename, ','.join(_fields))
                if _values:
                    self.values(_values)
            elif type(_fields) == types.DictType:
                keys = []
                values = []
                for k,v in _fields.iteritems():
                    keys.append(k)
                    values.append(self._value(v))
                self._sql = "insert into %s(%s) values (%s)" % (self._tablename, 
                    ','.join(keys), ','.join(values))
            else: # string
                self._sql = "insert into %s(%s)" % (self._tablename, _fields)
                if _values:
                    self.values(_values)
        else:
            self._sql = "insert into %s set" % (self._tablename)
            if kwargs:
                self.set(None, **kwargs)

        return self

    def other(self, s):
        self._sql += s
        return self

    def status(self, _like=None):
        if _like:
            self._sql = "show status like '%s'" % _like
        else:
            self._sql = "show status"
        return self

    def variables(self, _like=None):
        if _like:
            self._sql = "show variables like '%s'" % _like
        else:
            self._sql = "show variables"
        return self

    def values(self, _values):
        tv = type(_values)
        if tv in self._strtype:
            self._sql += " values (%s)" % _values
        elif tv in self._listtype:
            x = []
            for v in _values:
                x.append(self._value(v))
            self._sql += " values (%s)" % ','.join(x)
        else:
            raise ValueError, _values
        return self

    def limit(self, length, start=None):
        if start:
            self._sql += ' limit %d,%d' % (start, length)
        else:
            self._sql += ' limit %d' % length
        return self

    def commit(self):
        if self._sql:
            self._sql += ";commit"
        else:
            self._sql = "commit"
        return self

    def _pair(self, k, v, escape=True):
        #log.info('paire k:', k, 'v:', v)
        tv = type(v)
        if tv in self._strtype:
            if tv == types.UnicodeType:
                v = v.encode(self.charset)
            if v.startswith(('now()','md5(')):
                return "%s=%s" % (k, v)
            if escape:
                return "%s='%s'" % (k, self._conn.escape(v))
            return "%s='%s'" % (k, v)
        elif isinstance(v, datetime.datetime):
            return "%s='%s'" % (k, str(v))
        else:
            if v is None:
                return "%s=NULL" % (k)
            return "%s=%s" % (k, str(v))

    def _value(self, v, escape=True):
        #log.info('vaule v:', v)
        tv = type(v)
        if tv in self._strtype:
            if tv == types.UnicodeType:
                v = v.encode(self.charset)
            if v.startswith(('now()','md5(')):
                return v
            if escape:
                return "'%s'" % self._conn.escape(v)
            return "'%s'" % v
        elif isinstance(v, datetime.datetime):
            return "'%s'" % str(v)
        else:
            if v is None:
                return "NULL"
            return str(v)

    def tocount(self):
        if not self._sql:
            return self
        pos = self._sql.find(' from ')
        self._sql = "select count(*) as count " + self._sql[pos:]
        return self
  
    @_conn_selector
    def pager(self, page=1, pagesize=20):
        log.info('pager sql:%s', self._sql)
        p = pager.db_pager(self._conn, self._sql, page, pagesize)
        return p

    @_conn_selector
    def execute(self):
        if not self._sql:
            return None
        log.info('%s: %s', self._conn.conn, self._sql)
        return self._conn.execute(self._sql)

    @_conn_selector
    def query_list(self, isdict=True):
        if not self._sql:
            return []
        log.info('%s: %s', self._conn.conn, self._sql)
        return self._conn.query(self._sql, isdict=isdict)

    @_conn_selector
    def query(self, isdict=True):
        if not self._sql:
            return []
        log.info('%s: %s', self._conn.conn, self._sql)
        ret = self._conn.query(self._sql, isdict=isdict)
        return globals()['TableRecord'](self.model, ret)



class TableRow:
    UPDATE = 1
    INSERT = 2

    def __init__(self, _model, row=None, conn=None):
        #log.info('row:', row, 'table:', table)
        self._conn    = conn
        self._fields = set()
        self._model   = _model
        self._action  = self.UPDATE
        self._row     = row
        if not row:
            self._action = self.INSERT
        else:
            self.__dict__.update(row)

        #self._TableRow__setattr__ = self._setattr

    def __setattr__(self, k, v):
        #log.info('set attr:', k, v)
        self.__dict__[k] = v
        if not k.startswith('_'):
            self._fields.add(k)

    def __setitem__(self, k, v):
        self.__dict__[k] = v
        if not k.startswith('_'):
            self._fields.add(k)

    def __getitem__(self, k):
        return self.__dict__[k]

    def fields(self):
        if self._row:
            return self._row.keys()
        else:
            return self._model.fields()


    def field_choice_one(self, k):
        return getattr(self._model, k).pair(self.__dict__[k])

    def field_choice(self, k):
        return getattr(self._model, k).choice

    def todict(self):
        x = {}
        for k,v in self.__dict__.iteritems():
            if k.startswith('_'):
                continue
            if type(v) == types.FunctionType:
                continue
            if isinstance(v, datetime.datetime):
                x[k] = str(v)
                continue
            x[k] = v
        return x

    def save(self, commit=False):
        row = {}
        t = TableQuery(self._model, self._conn)
        if self._action == self.INSERT:
            for k,v in self.__dict__.iteritems():
                if k[0] == '_':
                    continue
                row[k] = v
            if commit:
                ret = t.insert(row).commit().execute()
            else:
                ret = t.insert(row).execute()
        else:
            #log.info('fields:', self._fields)
            if len(self._fields) == 0:
                return 0 
            for k in self._fields:
                row[k] = self.__dict__[k]
            if commit:
                ret = t.update(row).where(id=self.__dict__['id']).commit().execute()
            else:
                ret = t.update(row).where(id=self.__dict__['id']).execute()
        return ret

    def delete(self, commit=False):
        t = TableQuery(self._model, self._conn)
        if commit:
            return t.delete(id=self.__dict__['id']).commit().execute()
        else:
            return t.delete(id=self.__dict__['id']).execute()
        

    def __str__(self):
        row = []
        for k,v in self.__dict__.iteritems():
            if k[0] == '_':
                continue
            row.append('%s=%s' % (k,str(v)))
        return '<TableRow %s>' % ' '.join(row)


class TableRecord:
    def __init__(self, model, rets, conn=None):
        self._conn  = conn
        self._model = model
        self.data   = rets

    def __iter__(self):
        for row in self.data:
            yield TableRow(self._model, row, self._conn)

    def __str__(self):
        return '<TableRecord %d>\n' % len(self.data) + pprint.pformat(self.data)

    def __len__(self):
        if not self.data:
            return 0
        return len(self.data)

    def __getitem__(self, k):
        return TableRow(self._model, self.data[k], self._conn)

    def todict(self):
        x = []
        for row in self.data:
            newrow = {}
            for k,v in row.iteritems(): 
                if isinstance(v, datetime.datetime):
                    newrow[k] = str(v)
                    continue
                newrow[k] = v
            x.append(newrow)
        return x

class TableField:
    name = ''
    type = None
    maxlen = 0
    isnull = False
    key  = ''
    default = ''
    extra = ''

    def __str__(self):
        return 'name:%s type:%s maxlen:%s isnull:%s key:%s default:%s extra:%s' % \
            (self.name, self.type, str(self.maxlen), str(self.isnull), 
             self.key, repr(self.default), self.extra)
    
    @classmethod
    def load(cls, row):
        obj = cls()
        obj.name = row[0]
        pos = row[1].find('(')
        if pos == -1:
            obj.type = row[1]
            obj.maxlen = 0
        else:
            obj.type = row[1][:pos]
            obj.maxlen = int(row[1][pos+1:].strip(')'))

        if row[2] == 'YES':
            obj.isnull = True
        else:
            obj.isnull = False

        obj.key = row[3]
        #log.info('default type:', row[4], type(row[4]))
        #print row[4], type(row[4])

        if row[4] == 'NULL' or row[4] is None:
            obj.default = None
        elif obj.type in ['varchar','char','datetime','date','time','blob','text','tinyblob','tinytext','longblob','longtext']:
            obj.default = row[4]
        elif obj.type in ['int','tinyint','longint','mediumint']:
            obj.default = int(row[4])
        elif obj.type in ['float','double','real','decimal','numeric']:
            obj.default = float(row[4])
        else:
            obj.default = row[4]

        obj.defalut = row[4]
        obj.extra = row[5]
        return obj


class TableInfo:
    def __init__(self, table, conn=None):
        self.fields = {}
        self.table  = table
        self.conn   = conn
        self.uptime = 0
        self.load()

    def load(self):
        sql = "desc %s" % self.table 
        retx = self.conn.query(sql, isdict=False)
        #log.info(pprint.pformat(retx))
        for row in retx:
            f = TableField.load(row)
            self.fields[f.name] = f
        self.uptime = int(time.time())
 
    def __str__(self):
        s = []
        for x in self.fields:
            s.append(str(x))
        return ','.join(s)

    def topy(self, dbname):
        typemap = {'varchar':'TVarchar', 'char':'TChar', 'datetime':'TDateTime', 'date':'TDate', 'time':'TTime', 
                   'blob':'TBlob', 'text':'TText', 'tinyblob':'TTinyBlob', 'tinytext':'TTinyText', 'longblob':'TLongBlob', 
                   'longtext':'TLongText', 'int':'TInt', 'tinyint':'TTinyInt', 'longint':'TLongInt', 'mediumint':'TMediumInt', 
                   'float':'TFloat', 'double':'TDouble', 'real':'TReal', 'decimal':'TDecimal', 'numeric':'TNumeric'}
        tab = '    '
        lines = []

        def tablename(t):
            parts = [string.capwords(x) for x in t.split('_')]
            return ''.join(parts)

        lines.append("class %s (TableModel):" % (tablename(self.table)))
        lines.append("%s_tablename = '%s'" % (tab, self.table))         
        lines.append("%s_dbname = '%s'\n" % (tab, dbname))         

        for k,fi in self.fields.iteritems():
            print str(fi)
            attr = []
            attr.append("%s" % (typemap[fi.type]))
            if fi.type.endswith('char') and fi.maxlen > 0:
                if fi.default is None:
                    attr[-1] = attr[-1] + '(%d)' % (fi.maxlen)
                else:
                    attr[-1] = attr[-1] + '(%d, default=%s)' % (fi.maxlen, Value(fi.default))
            else:
                if fi.default is None:
                    attr[-1] = attr[-1] + '()'
                else:
                    attr[-1] = attr[-1] + '(default=%s)' % (Value(fi.default))

            if fi.isnull:
                attr.append("isnull=True")
            if fi.key == 'PRI':
                attr.append("primary_key=True")
            elif fi.key == 'UNI':
                attr.append("unique=True")
            elif fi.key == 'MUL':
                attr.append("key=True")
            if fi.extra == 'auto_increment':
                attr.append("autoinc=True")

            if fi.name == 'id':
                lines.insert(3, "%s%s = Column(%s)" % (tab, fi.name, ', '.join(attr)))
            else:
                lines.append("%s%s = Column(%s)" % (tab, fi.name, ', '.join(attr)))

        return '\n'.join(lines) + '\n'




# table info cache. dbname: {tablename:info}
_tableinfos = {}
def tableinfo(dbname, table, conn):
    global _tableinfos
    tbl = _tableinfos.get(dbname)
    if not tbl:
        tbl = {}
        _tableinfos[dbname] = tbl
    tinfo = tbl.get(table)
    if not tinfo:
        tinfo = TableInfo(table, conn)
    return tinfo

def dbinfo(conn):
    result = []
    sql = "show tables"
    rets = conn.query(sql, isdict=False)
    for row in rets:
        t = TableInfo(row[0], conn) 
        result.append(t)
    return result



def test1():
    log.install('ScreenLogger')
    DATABASE = {'forcoder': # connection name, used for getting connection from pool
                {'engine':'mysql',      # db type, eg: mysql, sqlite
                 'db':'forcoder',       # db table
                 'host':'127.0.0.1', # db host 
                 'port':3306,        # db port
                 'user':'forcoder',      # db user
                 'passwd':'123456',# db password
                 'charset':'utf8',# db charset
                 'timeout':10,
                 'init_command':'SET autocommit=0;',
                 'conn':20}          # db connections in pool
           }   

    dbpool.install(DATABASE)
    conn = dbpool.acquire('forcoder')
    try:
        tinfo = TableInfo("users", conn)
        log.info('table info:%s', tinfo)

        t = TableQuery("users", conn)
        t = t.select("id,username").where("id=1")
        print t._sql
        print 'exec:', t.query()
        t = t.update(id=100).where("id=2")
        print t._sql
        print 'exec:', t.execute()
        t = t.update("username='zhaowei'").where("id=3")
        print t._sql
        t = t.delete("username='bobo'")
        print t._sql
        t = t.insert("id,username,password").values("1,'zhaowei','bobo'")
        print t._sql
        t = t.insert(id=1,username='zhaowei')
        print t._sql
        t = t.insert(['id','username']).values([1,'zhaowei'])
        print t._sql
        t = t.select("id,username").where(id=1, name='zhaowei')
        print t._sql
        t = t.select("id,username").where(Or(And("id>1", "id<5"), "id>10"))
        print t._sql
        t = t.select("id,username").where(Or(Or(And(Great('id',1), Less('id',5)), GreatEqual('id',10)), 
                Equal('username', 'zhaowei')))
        print t._sql
        t = t.select("id,username").where(Or(And("id>1", "id<5"), "id>10", In('username', 'zhaowei1','zhaowei2')))
        print t._sql
        t = t.status('haha%')
        print t._sql
        t = t.variables()
        print t._sql

    finally:
        dbpool.release(conn) 

def test2():
    log.install('ScreenLogger')
    DATABASE = {'forcoder': # connection name, used for getting connection from pool
                {'engine':'mysql',      # db type, eg: mysql, sqlite
                 'db':'forcoder',       # db table
                 'host':'127.0.0.1', # db host 
                 'port':3306,        # db port
                 'user':'forcoder',      # db user
                 'passwd':'123456',# db password
                 'charset':'utf8',# db charset
                 'timeout':10,
                 #'init_command':'SET autocommit=1;',
                 'autocommit':True,
                 'conn':20}          # db connections in pool
           }   

    dbpool.install(DATABASE)
    conn = dbpool.acquire('forcoder')
    try:
        for i in range(1, 10):
            trow = TableRow('users', conn)
            trow.id = i
            trow.username = 'zhaowei%d' % i
            trow.password = "md5('aaaaa')"
            trow.email    = 'zhaoweikid%d@163.com' % i
            log.info('save:%s', trow.save())

        t = TableQuery('users', conn)
        rets = t.select("*").where(id=1).query()
        for ret in rets:
            log.info(ret)
            ret.uptime = 'now()'
            ret.save()
    finally:
        dbpool.release(conn) 


def test3():
    log.install('ScreenLogger')
    DATABASE = {'forcoder': # connection name, used for getting connection from pool
                {'engine':'mysql',      # db type, eg: mysql, sqlite
                 'db':'forcoder',       # db table
                 'host':'127.0.0.1', # db host 
                 'port':3306,        # db port
                 'user':'forcoder',      # db user
                 'passwd':'123456',# db password
                 'charset':'utf8',# db charset
                 'timeout':10,
                 #'init_command':'SET autocommit=1;',
                 'autocommit':True,
                 'conn':20}          # db connections in pool
           }   

    dbpool.install(DATABASE)
    conn = dbpool.acquire('forcoder')
    try:
        #x = TableInfo('users', conn)
        #print x.topy()
        ts = dbinfo(conn)
        for x in ts:
            print x.topy()
    finally:
        dbpool.release(conn) 


if __name__ == '__main__':
    test3()

