# coding: utf-8
import os, sys
import multiprocessing
import time
import urllib2

def timeit(func):
    def _(wid, addr, reqnum, longconn):
        tstart = time.time()
        ret = func(wid, addr, reqnum, longconn)
        tend = time.time()
        t = tend - tstart
        qps = int(reqnum / t)
        resptime = sum([ x[1] for x in ret]) / reqnum
        maxtime = max([ x[1] for x in ret ])
        print 'id:%d time:%f qps:%-6d resptime:%f maxtime:%f' % \
                (wid, t, qps, resptime, maxtime)

        return ret
    return _

@timeit
def do_work(wid, addr, reqnum, longconn):
    tstart = time.time()
    print 'haha'
    tend = time.time()

    ret = [[tstart, tend-tstart],]
    return ret

def usage():
    print 'perf.py 请求地址 客户端数 每客户端请求数 是否长连接'

def main():
    if len(sys.argv) != 5:
        usage()
        return

    addr, client, reqnum, longconn = sys.argv[1:] 
    print '请求地址: %s' % addr
    print '客户端数量: %s' % client
    print '每客户端请求数: %s' % reqnum
    print '是否长连接: %s' % longconn
    
    args = dict(zip(['addr', 'client', 'reqnum', 'longconn'], 
                    [addr, int(client), int(reqnum), int(longconn)]))
    jobs = []
    for i in range(0, args['client']):
        pargs = [i, args['addr'], args['reqnum'], args['longconn']]
        p = multiprocessing.Process(target=do_work, args=pargs)
        jobs.append(p)
    for p in jobs:
        p.start()

    for p in jobs:
        p.join()
    print 'end'

if __name__ == '__main__':
    main()


