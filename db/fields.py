# coding: utf-8
import os, sys
import types
import re

# attr
ADD     = 0x01
EDIT    = 0x02
LIST    = 0x04
FIND    = 0x08
LIKE    = 0x10

# input type match
TypeInt     = re.compile('^\-?[0-9\,]+$')
TypeLong    = re.compile('^\-?[0-9\,]+$')
TypeFloat   = re.compile('^\-?[0-9\,]+(\.[0-9\,]+)?$')
TypeBool    = re.compile('^(True|False|true|false|TRUE|FALSE)$')
TypeText    = re.compile('^.*$', re.DOTALL)
TypeExText  = re.compile('^.*$', re.DOTALL)
TypeStr     = re.compile('^.*$')
# 非空字符串
TypeExStr   = re.compile('^.+$')
# 可见ascii码字符串
TypeAscStr  = re.compile('^[\x20-\x80]+$')
# 受限制的字符串，只能包含英文字符，数字和_-.
TypeVarStr  = re.compile('^[a-zA-Z0-9\-_\.]+$')
# 仅仅英文字符
TypeEnStr   = re.compile('^[a-zA-Z]+$')
TypeEmail   = re.compile('^[a-zA-Z0-9-_\.\']+@[a-zA-Z0-9\-\.]+')
TypeIp      = re.compile('^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$')
TypeTime    = re.compile('^[0-9:]+$')
TypeDate    = re.compile('^[0-9\-/]+$')
#DateTimeType = re.compile('^[0-9\-/:, ]+|now\(\)$')
TypeDateTime= re.compile('^[0-9\-/:, ]+$')


class TBase:
    typestr = ''
    maxlen  = 0
    default = None
    choice  = None

    def __init__(self, default=None, choice=None):
        self.default = default
        self.choice  = choice

    def __str__(self):
        return "<%s %s %d %s>" % (self.__class__.__name__, self.typestr, self.maxlen, str(self.default))

    def isstring(self):
        if self.typestr.find('char') >= 0 or self.typestr.endswith('text'):
            return True
        return False

    def isnumber(self):
        if self.typestr.endswith('int') or self.typestr in ['float','double','real','decimal','numeric']:
            return True
        return False

    def isinteger(self):
        if self.typestr.endswith('int'):
            return True
        return False

    def isfloat(self):
        if self.typestr in ['float','double','real','decimal','numeric']:
            return True
        return False

    def isdate(self):
        if self.typestr in ['date','datetime','time']:
            return True
        return False

    def isbinary(self):
        if self.typestr.endswith('blob'):
            return True
        return False

class TLenBase(TBase):
    def __init__(self, maxlen, default=None, choice=None):
        self.max = maxlen
        TBase.__init__(self, default=default, choice=choice)

class TChar (TBase):
    typestr = 'char'

    def __init__(self, maxlen, default=None, choice=None):
        self.maxlen  = maxlen
        self.default = default
        self.choice  = choice

class TVarchar (TBase):
    typestr = 'varchar'

    def __init__(self, maxlen, default=None, choice=None):
        self.maxlen  = maxlen
        self.default = default
        self.choice  = choice

class TEnum (TBase):
    typestr = 'enum'
    
class TInt (TBase):
    typestr = 'int'

class TTinyInt (TBase):
    typestr = 'tinyint'

class TMediumInt (TBase):
    typestr = 'mediumint'

class TBigInt (TBase):
    typestr = 'bigint'

class TFloat (TBase):
    typestr = 'float'

class TDouble (TBase):
    typestr = 'double'

class TReal (TBase):
    typestr = 'real'

class TDecimal (TBase):
    typestr = 'decimal'

class TNumeric (TBase):
    typestr = 'numeric'

class TText (TBase):
    typestr = 'text'

class TTinyText (TBase):
    typestr = 'tinytext'

class TLongText (TBase):
    typestr = 'longtext'

class TBlob(TBase):
    typestr = 'blob'

class TTinyBlob(TBase):
    typestr = 'tinyblob'

class TLongBlob(TBase):
    typestr = 'longblob'

class TDateTime (TBase):
    typestr = 'datetime'

class TDate (TBase):
    typestr = 'date'

class TTime (TBase):
    typestr = 'time'


# name, default, isnull, match, attribute, error, key, method, primary_key
class Column:
    def __init__(self, typex, **options):
        self.name  = '' 
        self.table = ''
        self.type  = typex
        self.isnull= False
        self.key   = False
        self.primary_key = False
        self.autoinc     = False
        self.unique      = False
        
        # extension
        self.show   = '' # 字段显示名
        self.method = '' # http 方法
        self.match  = '' # 正则匹配
        self.attr   = None # 字段属性，用来过滤
        self.error  = '' # 错误信息
        self.choice = None # 值选项

        self.__dict__.update(options)

        if self.match and type(self.match) in [types.StringType, types.UnicodeType]:
            self.match = re.compile(self.match)
          
        if not self.match:
            if self.type.isinteger():
                self.match = TypeInt
            elif self.type.isfloat():
                self.match = TypeFloat
            elif self.type.isstring():
                self.match = TypeStr
            elif self.type.isdate():
                self.match = TypeDateTime

    def update(self, **options):
        self.__dict__.update(options)

    def __str__(self):
        return "<Colum %s.%s %s>" % (self.table, self.name, self.type)

    def __repr__(self):
        return "<Colum %s.%s %s>" % (self.table, self.name, self.type)


    def pair(self, v):
        if not self.choice:
            return (v, '')
        for x in self.choice:
            if x[0] == v:
                return x
        return (v, '')

class Field (Column):
    def __init__(self, **options):
        Column.__init__(self, None, **options)


opmap = {'eq':'=', 'lt':'<', 'gt':'>', 'ne':'<>', 'le':'<=', 'ge':'>=', 'like':' like '}


class InputItem:
    def __init__(self, key, op, val):
        # op  eq:=,lt:<,gt:>,ne:!=,le:<=,ge:>=,like
        self.k  = key
        self.op = op
        self.v  = None
        self.t  = None
        self.vl = val

        if val:
            self.v = val[0]

    def __str__(self):
        return '<InputItem %s %s %s>' % (self.k, self.op, str(self.vl))

    @classmethod
    def fromDict(cls, key, val):
        if key.find('__') <= 0:
            return cls(key, '=', val)
        p = k.split('__')
        return cls(p[0], opmap[p[1]], val)

    def setval(self, val):
        self.v  = val[0]
        self.vl = val

InputNone = InputItem('','',[''])


def dict2inputitem(data):
    ret = {}
    for k,v in data.iteritems():
        data[k] = InputItem.fromDict(k, v)
    return ret



