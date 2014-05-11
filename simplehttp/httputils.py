# ncoding: utf-8
import os, sys, string, re
import time, types
import urlparse, locale
import pickle

charset = locale.getdefaultlocale()
if sys.platform.startswith('darwin'):
    try:
        charset = os.environ['LANG'].split('.')
    except:
        charset[1] = 'UTF-8'


topdomain = ['com', 'net', 'org', 'edu', 'gov', 'mil', 
             'aero', 'biz', 'cat', 'tv', 'cc', 'la', 
             'coop', 'info', 'int', 'jobs', 'mobi', 
             'museum', 'name', 'pro', 'travel', 'me']

def domain_get (url):
    urlsp = urlparse.urlparse(url) 
    if not urlsp[1]:
        return ''
    mao = string.find(urlsp[1], ':')
    if mao != -1:
        tmpd = urlsp[1][0:mao] 
    else:
        tmpd = urlsp[1]
    dots = string.split(tmpd, '.')
    if len(dots) < 2:
        return ''
    try:
        a = int(dots[-1])
    except:
        if len(dots[-1]) == 2: # 国家代码顶级域
            if dots[-2] in topdomain:
                if len(dots) >= 3:
                    domain = "%s.%s.%s" % (dots[-3], dots[-2], dots[-1])
                else:
                    domain = "%s.%s" % (dots[-2], dots[-1])
            else:
                domain = "%s.%s" % (dots[-2], dots[-1])
        elif dots[-1] in topdomain: # 顶级域
            domain = "%s.%s" % (dots[-2], dots[-1])
        else:
            return ''

    else: # 是ip地址
        domain = urlsp[1]
    
    return domain

def strip_html(data):
    '''去除data里的所有html标签'''
    return newdata

if __name__ == '__main__':
    a = "http://www.baidu.com.cn/ass/fefef/awera/dfasjlfa?py=222"
    print a
    print domain_get(a)
    a = "http://www.51.la/cafasd/fadsf/erwre/w/r?af=1"
    print a
    print domain_get(a)

