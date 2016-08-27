# coding: utf-8
import re
import types
import logging
import traceback

log = logging.getLogger()

T_INT       = 1
T_STR       = 2
T_FLOAT     = 4
T_DOUBLE    = T_FLOAT
T_REG       = 8
T_MAIL      = 16
T_IP        = 32
T_MOBILE    = 64


TYPE_MAP = {
    T_MAIL: "^[a-zA-Z0-9_\-\'\.]+@[a-zA-Z0-9_]+(\.[a-z]+){1,2}$",
    T_IP: "^([0-9]{1,3}\.){3}[0-9]{1,3}$",
    T_MOBILE: "^1[3578][0-9]{9}$"
}
#T_LIST  = 16

opmap = {'eq':'=',
         'lt':'<',
         'gt':'>',
         'ne':'<>',
         'le':'<=',
         'ge':'>=',
         'in':'in',
         'bt':'between',
         'lk':'like',
         }

class Field:
    def __init__(self, name, valtype=0, isnull=True, default = '', **options):
        self.name   = name
        self.type   = valtype # 值类型, 默认为字符串
        self.isnull = isnull # 是否可以为空
        self.op     = '='
        self.default= default

        # 扩展信息
        self.show   = '' # 字段显示名
        self.method = '' # http 方法
        self.match  = '' # 正则匹配
        self.attr   = None # 字段属性，用来过滤
        self.error  = ''   # 错误信息
        self.choice = None # 值枚举值
        self.value  = None

        self.__dict__.update(options)

        if self.match and type(self.match) in [types.StringType, types.UnicodeType]:
            self.match = re.compile(self.match)




class ValidatorError (Exception):
    pass

class Validator:
    def __init__(self, fields=None):
        # fields must have isnull,type,match,name
        self._fields = fields
        self.data = {}

    def _check_item(self, field, val):
        if field.type & T_INT:
            return int(val)
        elif field.type & T_FLOAT:
            return float(val)
        elif field.type & T_STR:
            return val
        elif field.type & T_REG:
            if not field.match.match(val):
                log.debug('validator match error: %s, %s', field.match.pattern, str(val))
                raise ValidatorError(field.name)
        elif field.type in TYPE_MAP:
            fm = TYPE_MAP[field.type]
            if not fm.match(val):
                log.debug('validator match error: %s, %s', fm.pattern, str(val))
                raise ValidatorError(field.name)
        # 缺省是字符串
        return val

    def check(self, inputdata):
        '''不建议再使用，应该使用新的verify'''
        result = [] # 没验证通过的字段名
        for k,v in inputdata.iteritems():

            if v is None:
                continue

            try:
                for f in self._fields:
                    if '__' in k:
                        k_name,k_op = k.split('__')
                        if k_name == f.name:
                            if v == '' and f.isnull:
                                continue
                            if k_op in opmap.keys():
                                f.op = opmap[k_op]
                                if ',' in v:
                                    val = v.split(',')
                                    f.value = [self._check_item(f,cv) for cv in val]
                                else:
                                    f.value = self._check_item(f,v)
                                self.data[f.name] = f.value
                            else:
                                result.append(k)
                                continue
                    else:
                        if k == f.name:
                            if v == '' and f.isnull:
                                continue
                            if ',' in v:
                                val = v.split(',')
                                f.value = [self._check_item(f,cv) for cv in val]
                                f.op = 'in'
                            else:
                                f.value = self._check_item(f,v)
                            self.data[f.name] = f.value

            except:
                result.append(k)
                log.info(traceback.format_exc())
        return result


    def _verify_item_type(self, field, val):
        if field.type & T_INT:
            return int(val)
        elif field.type & T_FLOAT:
            return float(val)
        elif field.type & T_STR:
            return val
        elif field.type & T_REG:
            if not field.match.match(val):
                return None
        # 缺省是字符串
        return val


    def verify(self, inputdata):
        result = [] # 没验证通过的字段名

        # check input format and transfer to {key: [op, value]}
        _input = {}
        for k,v in inputdata.iteritems():
            if '__' in k:
                k_name,k_op = k.split('__')
                op = opmap.get(k_op)
                if not op: # k_name error
                    result.append(k_name)
                    continue
                _input[k_name] = [op, v]
            else:
                _input[k] = ['=', v]

        if result:
            return result

        # check field and transfer type
        for f in self._fields:
            try:
                val = _input.get(f.name)
                if not val: # field defined not exist
                    if not f.isnull: # null is not allowed, error
                        result.append(f.name)
                    else:
                        f.value = f.default
                        self.data[f.name] = f.default
                    continue

                f.op = val[0]
                v = val[1]  
                if ',' in v: # , transfer to list
                    val = v.split(',')
                    f.value = [self._check_item(f,cv) for cv in val]
                    if not f.value:
                        result.append(f.name)
                else:
                    f.value = self._check_item(f, v)
                    if f.value is None:
                        result.append(f.name)
                self.data[f.name] = f.value
            except ValidatorError:
                result.append(f.name)
                log.warn(traceback.format_exc())
            except:
                result.append(f.name)
                log.info(traceback.format_exc())
        return result

    def report(self, result, sep=u'<br/>'):
        ret = []
        for x in result:
            if x:
                ret.append(u'"%s"错误!' % x)
        return sep.join(ret)


def check_validator(fields):
    def f(func):
        def _(self, *args, **kwargs):
            vadt = Validator(fields)
            ret = vadt.check(self.request.arguments)
            if ret:
                return vadt.report(ret)
            self.validator = vadt
            return func(self, *args, **kwargs)
        return _
    return f


def with_validator(fields, errfunc=None):
    def f(func):
        def _(self, *args, **kwargs):
            vdt = Validator(fields)
            ret = vdt.verify(self.req.input())
            log.debug('validator check:%s', ret)
            if ret:
                #log.debug('err:%s', errfunc(ret))
                if errfunc:
                    return errfunc(self, ret)
                else:
                    self.resp.status = 400
                    return 'input error'
            self.validator = vdt
            return func(self, *args, **kwargs)
        return _
    return f

def with_validator_self(func):
    def _(self, *args, **kwargs):
        vdt = Validator(getattr(self, '%s_fields'% func.__name__))
        ret = vdt.verify(self.req.input())
        log.debug('validator check:%s', ret)
        if ret:
            #log.debug('err:%s', errfunc(ret))
            errfunc = getattr(self, '%s_errfunc'% func.__name__, None)
            if errfunc:
                return errfunc(ret)
            else:
                self.resp.status = 400
                return 'input error'
        self.validator = vdt
        return func(self, *args, **kwargs)
    return _




def test1():
    fields = [Field('age', T_INT),
              Field('money', T_FLOAT),
              Field('name'),
              Field('cate', T_INT),
              Field('income',T_INT),
              Field('test',T_INT),
              ]

    input = {'name':'aaaaa', 'age':'12', 'money':'12.44',
             'cate__in':'1,2,3', 'income__bt':'1000,5000',
             'no_tesst':'123'}

    x = Validator(fields)
    ret = x.check(input)

    if ret:
        for q in ret:
            print q
    else:
        print 'check ok'

    for f in x._fields:
        print 'name:%s, value:%s, valuetype:%s, op:%s'%(f.name, f.value, type(f.value), f.op)

def test2():
    fields = [Field('age', T_INT),
              Field('money', T_INT),
              Field('name'),
              ]


    Validator(fields)

    class Test:
        GET_fields = [Field('age', T_INT),
              Field('money', T_INT),
              Field('name'),
              ]


        def __init__(self):
            self.input = {'name':'aaaaa', 'age':'12', 'money':'12.44'}

        @check_validator
        def GET(self):
            log.info('testfunc ...')




    t = Test()
    t.testfunc()
    log.info('after validator: %s', t.validator.data)

def test3():
    fields = [
        Field('age', T_INT, isnull = True, default = 18),
        Field('name', T_STR, isnull = False),
        Field('money', T_INT),
    ]
    input = {'name': 'aaaa', 'money': '12'}
    v = Validator(fields)
    ret = v.verify(input)
    print ret
    print v.data
    fields = [
        Field('age', T_INT, isnull = True, default = 18),
        Field('name', T_STR, isnull = False),
        Field('money', T_INT),
        Field('title', T_REG, match = '.{3,20}'),
    ]
    input['title'] = '1111111'
    v = Validator(fields)
    ret = v.verify(input)
    print ret
    print v.data


def test4():
    class Test:
        def __init__(self):
            self.input = {'name':'aaaaa', 'age':'12', 'money':'12.44'}

        @with_validator([Field('age', T_INT), Field('money', T_INT), Field('name'),])
        def testfunc(self):
            log.info('testfunc ...')

    t = Test()
    t.testfunc()
    log.info('after validator: %s', t.validator.data)




if __name__ == '__main__':
    #test1()
    #test2()
    #test3()
    test4()


