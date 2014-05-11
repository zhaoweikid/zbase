# coding: utf-8
import sys, os
import copy, traceback
import math
from zbase.base.logger import log

class Pager:
    '''分页类'''
    def __init__(self, data, page, pagesize):
        '''设置初始值
        data - data
        page - 当前页码
        pagesize - 每页显示条数
        '''
        self.pagedata = data
        if page <= 0:
            page = 1
        self.page = page

        self.count = -1
        self.page_size = pagesize
        self.pages = 0

    def split(self):
        if self.count == -1:
            self.count, self.pages = self.pagedata.count(self.page_size)
        
        self.pagedata.load(self.page, self.page_size)

    def todict(self):
        log.info('data:', self.pagedata.data)
        x = copy.copy(self.pagedata.data)
        for row in x:
            for k,v in row.iteritems():
                if k.endswith('time'):
                    row[k] = str(v)
        return x

    def prev(self):
        if self.page == 1:
            return 0
        else:
            return self.page-1

    def have_prev(self):
        if self.page <= 1:
            return False
        return True

    def next(self):
        if self.pages > 0 and self.page >= self.pages:
            return 0
        else:
            return self.page+1

    def have_next(self):
        if self.pages > 0 and self.page >= self.pages:
            return False
        return True

    def first(self):
        return 1
   
    def last(self):
        if self.pages <= 0:
            return 1
        return self.pages
                                                    
    def range(self, maxlen=10):
        if self.pages > 0:
            pagecount = self.pages
        else:
            pagecount = self.page + maxlen
        ret = range(max(self.page-maxlen, 1), min(self.page+maxlen, pagecount)+1)
        #log.info("range:", ret)
        return ret

    def pack(self):
        r = {'prev':  self.prev(),
             'next':  self.next(),
             'first': self.first(),
             'last':  self.last(),
             'pages': self.pages,
             'page':  self.page,
             'count': self.count,
             'range': self.range}

        return r

    def show_html(self):
        pass

class PageDataBase:
    def load(self, cur, pagesize):
        pass

    def count(self, pagesize):
        pass

class PageDataDB (PageDataBase):
    def __init__(self, db, sql, count_sql=None, maxid=-1):
        '''设置初始值
        db  - 数据库连接对象
        sql - 分页查询sql
        pagesize - 每页显示条数
        maxid - 最大id
        '''
        self.db   = db
        self.data = []
        self.url  = ''
        self.maxid = maxid
        
        sql = sql.replace('%', '%%')
        # 如果设置了最大id，在查询的时候要加上限制，但是这里有问题。可能原来的分页sql已经有where了
        if maxid > 0:
            self.query_sql = sql + " where id<" + str(maxid) + " limit %d"
        else:
            self.query_sql = sql + " limit %d,%d"
        
        # 生成计算所有记录的sql
        if count_sql:
            self.count_sql = count_sql
        else:
            backsql  = sql[sql.find(" from "):]
            orderpos = backsql.find(' order by ')
            # 去掉order，统计记录数order没用
            if orderpos > 0: 
                backsql = backsql[:orderpos]
            self.count_sql = "select count(*) as count " + backsql 
        self.records = -1

    def load(self, cur, pagesize):
        if self.maxid >= 0:
            sql = self.query_sql % (pagesize)
        else:
            sql = self.query_sql % ((cur-1)*pagesize, pagesize)
        log.info('PageDataDB load sql:', sql)
        self.data = self.db.query(sql)
        return self.data

    def count(self, pagesize):
        '''统计页数'''
        # 没有统计页数的sql，说明不需要计算总共多少页
        log.info("PageDataDB count sql:", self.count_sql)
        ret = self.db.query(self.count_sql)
        row = ret[0]
        self.records = int(row['count'])
        log.info("PageDataDB count:", self.records)
        a = divmod(self.records, pagesize)
        if a[1] > 0:
            page_count = a[0] + 1
        else:
            page_count = a[0]
        return self.records, page_count


def db_pager(db, sql, pagecur, pagesize, count_sql=None, maxid=-1):
    pgdata = PageDataDB(db, sql, count_sql, maxid)
    p = Pager(pgdata, pagecur, pagesize)
    p.split()
    return p



try:
    from memlink.memlinkclient import *
except:
    pass
else:
    
    def memlink_pager(conn, table, key, pagecur=1, pagesize=20, kind=MEMLINK_VALUE_VISIBLE):
        pgdata = PageDataMemlink(conn, key, kind)
        p = Pager(pgdata, pagecur, pagesize)
        p.split()
        return p

    class PageDataMemlink (PageDataBase):
        def __init__(self, conn, table, key, kind=MEMLINK_VALUE_VISIBLE):
            self.conn = conn
            self.table= table
            self.key  = key
            self.data = []
            self.kind = kind

        def load(self, cur, pagesize):
            ret, result = self.conn.range(self.table, self.key, self.kind, (cur-1)*pagesize, pagesize)
            if ret != MEMLINK_OK:
                log.err('range error, ret:%d, key:%s, cur:%d, pagesize:%d' % \
                        (ret, self.key, cur, pagesize))
                return []
            self.data = result.list()
            return self.data

        def data2strlist(self):
            for x in self.data:
                pos = x[0].find('\x00')
                if pos >= 0:
                    x[0] = x[0][:pos]
            return self.data

        def count(self, pagesize):
            ret, result = self.conn.count(self.table, self.key)
            if ret != MEMLINK_OK:
                log.err('count error, ret:%d, key:%s.%s' % (ret, self.table, self.key))
                return 0, 0

            if self.kind == MEMLINK_VALUE_VISIBLE:
                num = result.visible_count
            elif self.kind == MEMLINK_VALUE_TAGDEL:
                num = result.tagdel_count
            else: 
                num = result.visible_count + result.tagdel_count

            a = divmod(num, pagesize)
            if a[1] > 0:
                page_count = a[0] + 1
            else:
                page_count = a[0]
            return num, page_count


