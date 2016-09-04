# coding: utf-8
import types, json
from qfcommon.server.client import ThriftClient
import traceback
from dbpool import acquire, release, install
import logging
import logger
log = logging.getLogger()

# 裁剪卡号，前6后4
def mask_card(cardcd):
    if not isinstance(cardcd, types.StringTypes):
        return ''

    if len(cardcd) > 10:
        return cardcd[0:6] + '*' * (len(cardcd) - 10) + cardcd[-4:]
    else:
        return cardcd

#日志输出除去敏感信息
def escape(data):
    def _escape(indata):
        escape_fields = ['cardcd', 'incardcd', 'cardpin', 'iccdata', 'trackdata2', 'trackdata3', 'trackdata', 'zmk', 'zpk',
                        'outgoingacct', 'incomingacct','creditacntcd', 'signature','custmracnt','debitacntcd','expiredate',
                        'cardexpire', 'CARDNO', 'TRACKDATA3', 'TRACKDATA2', 'PINCIPHER', 'ICCDATA']
        result = {}
        for key, value in indata.iteritems():
            if key in escape_fields and value:
                if key in ['cardcd', 'outgoingacct', 'incomingacct', 'CARDNO']:
                    result[key] = mask_card(value)
                else:
                    result[key] = len(value)
            else:
                result[key] = value
        return result

    if type(data) == dict:
        return _escape(data)
    elif type(data) in types.StringTypes:
        try:
            result = json.loads(data)
        except:
            return data
        else:
            return _escape(result)
    else:
        return data

# 安全的数据库操作
def safe_operation(**kwargs):
    ''' 必须得参数:
            token, table, func
        根据func传递其他关键字参数
        连续多次查询不要使用
    '''
    if not kwargs.has_key('token'):
        log.error("func=safe_operation|error=token not found")
        return None
    token = kwargs.get('token')
    kwargs.pop('token')
    conn = None
    try:
        conn = acquire(token)
    except:
        log.error("server=mysql|error=%s", traceback.format_exc())
        return None
    if not conn:
        return None
    try:
        func_name = kwargs.get('func', '')
        if not func_name:
            log.error("server=mysql|error=not found func")
            return none
        kwargs.pop('func')
        func = getattr(conn, func_name)
        return func(**kwargs)
    except:
        log.error("server=mysql|error=%s", traceback.format_exc())
        return None
    finally:
        if conn:
            release(conn)

def thrift_call(thriftmod, funcname, server, *args, **kwargs):
    client = ThriftClient(server, thriftmod)
    return client.call(funcname, *args, **kwargs)


def thrift_callex(server, thriftmod, funcname, *args, **kwargs):
    client = ThriftClient(server, thriftmod)
    client.raise_except = True

    return client.call(funcname, *args, **kwargs)

def smart_utf8(strdata):
    ''' strdata转换为utf-8编码字符串'''
    return strdata.encode('utf-8') if isinstance(strdata, unicode) else str(strdata)


def test_safe_operation():
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'mysql',   # db type, eg: mysql, sqlite
                 'db':'qf_core',        # db name
                 'host':'172.100.101.151', # db host
                 'port':3306,        # db port
                 'user':'qf',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10},          # db connections in pool
                'trade': # connection name, used for getting connection from pool
                {'engine':'mysql',   # db type, eg: mysql, sqlite
                 'db':'qf_trade',        # db name
                 'host':'172.100.101.151', # db host
                 'port':3306,        # db port
                 'user':'qf',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10}          # db connections in pool

           }

    install(DATABASE)
    print safe_operation(token='test', func='select', where={'userid':227519}, table='chnlbind')
    print safe_operation(token='trade', func='select', where={'userid':227519, 'syssn':('like', '20140801')}, table='record_201408')
    print safe_operation(token='test', func='get', sql='select count(0) from auth_user')

if __name__=='__main__':
    test_safe_operation()
