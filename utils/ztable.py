#coding: utf-8
import string, sys, os
from UserDict import *
import threading, types
from simplehttp import httputils
from zbase.base.logger import log

class MDict (UserDict):
    def __init__(self):
        UserDict.__init__(self)

    def __setitem__(self, key, val):
        if self.data.has_key(key):
            v = self.data[key]
            v.append(val)
        else:
            self.data[key] = [val]

    def remove(self, key, value):
        v = self.data[key]
        if len(v) == 1:
            del self.data[key]
        else:
            v.remove(value)

    def modify(self, key, value, newval):
        v = self.data[key]
        ix = v.index(value)
        v[ix] = newval


class zTable:
    def __init__(self, fields=[], index={}):
        '''内存表, 参数fields表示字段名，index表示索引名，格式为{field:attr,}'''
        log.info('create ztable with fields:%s index:%s', fields, index)
        # 字段列表
        self._fields = fields
        # 字段映射表, {key: pos in fields}
        self._fields_map = {}
        # 索引属性，可以是u(unique), k(key), n(not null)
        self._index_attr = index
        # 索引数据
        self._index = {}
       
        # 记录
        self._record = []
        # 当前记录
        #self._cur = -1
        
        for i in xrange(0, len(self._fields)):
            x = self._fields[i]
            self._fields_map[x] = i

        for x,v in index.iteritems():
            if v.find('u') >= 0: # 只能是唯一关键字
                self._index[x] = {} # 里面保存的是{field value: record}
            elif v.find('k') >= 0: # 可以有重复关键字
                self._index[x] = MDict()
            else:
                self._index[x] = MDict()

        self._lock = threading.Lock()

    def __str__(self):
        s = 'records count: %d\nkey:\t%s\n' % (len(self._record), ','.join(self._fields_map.keys()))
        s += 'index:\t%s\nrecords:\n' % (','.join(self._index.keys()))
        for r in self._record:
            v = ''
            for fi in r:
                if type(fi) == types.UnicodeType:
                    v += fi.encode(httputils.charset[1]) + '\t'
                else:
                    v += str(fi) + '\t'
            s += v + '\n'
         
        return s

    def __len__(self):
        return len(self._record)

    def clear(self):
        '''清除记录以及索引内容，但是保留索引名称'''
        self._lock.acquire()
        try:
            self._record = []
            #self._cur = -1
            self._index.clear()
        except:
            raise
        finally:
            self._lock.release()
    
    #def reset(self):
    #    '''重置内部当前记录'''
    #    self._cur = -1

    def field_values(self, fieldname):
        v = []
        pos = self._fields_map[fieldname]
        for row in self._record:
            v.append(row[pos]) 
        return v

    def field_pos(self, fieldname):
        return self._fields_map[fieldname]
    
    def index_has_key(self, indexname, keyname):
        index = self._index[indexname]
        if index.has_key(keyname):
            return True
        return False

    def select(self, w={}):
        '''{'a': 1}'''
        self._lock.acquire()
        try:
            data = []
            if not w:
                return self._record
            # 遍历，应该尽可能使用索引
            for r in self._record:
                isok = True
                for k in w:
                    i = self._fields_map[k]
                    if r[i] != w[k]:
                        isok = False
                        break
                if isok:
                    data.append(r)
        except:
            raise
        finally:
            self._lock.release()
        return data
    
    def select_index(self, indexname, value):
        index = self._index[indexname]
        return index[value]
    
    def select_field(self, indexname, indexval, fieldname):
        index = self._index[indexname]
        pos   = self._fields_map[fieldname]
        return index[indexval][pos]

    def select_adv(self, w={}):
        '''{'a': 条件, 值}, 条件可以是like match < = <= > >= <>'''
        self._lock.acquire()
        try:
            data = []
            if not w:
                return self._record
            # 遍历，应该尽可能使用索引
            for r in self._record:
                isok = True
                for k in w:
                    i = self._fields_map[k]
                    x = w[k]
                    cond = x[0]
                    val = x[1]

                    if cond == 'like':
                        if string.find(r[i], val) < 0:
                            isok = False
                            break
                    elif cond == 'match': # 正则，待考虑
                        pass 
                    elif cond == '=':
                        if r[i] != val:
                            isok = False
                            break
                    elif cond == '<':
                        if not r[i] < val:
                            isok = False
                            break
                    elif cond == '<=':
                        if not r[i] <= val:
                            isok = False
                            break
                    elif cond == '>':
                        if not r[i] > val:
                            isok = False
                            break
                    elif cond == '>=':
                        if not r[i] >= val:
                            isok = False
                            break
                    elif cond == '<>':
                        if r[i] == val:
                            isok = False
                            break
                    else:
                        raise ValueError, "condition error!"
                if isok:
                    data.append(r)
        except:
            raise
        finally:
            self._lock.release()
        return data
            

    def insert(self, rec):
        '''插入一条记录'''
        log.info('insert:%s', rec)
        self._lock.acquire()
        try:
            # index需要保证值是唯一的, 并且属性是n的不可以为空
            for k in self._index_attr:
                attr = self._index_attr[k]
                i = self._fields_map[k]
                idx = self._index[k]
                if attr.find('u') >= 0 and idx.has_key(rec[i]):
                    raise KeyError, 'duplicate key: ' + k
                if attr.find('n') >= 0 and (rec[i] == '' or rec[i] == None):
                    raise KeyError, 'key %d must not null' % (i)

            self._record.append(rec)
            # 插入纪录需要更新所有的索引 
            for k in self._fields_map:
                if self._index_attr.has_key(k):
                    idx = self._index[k]
                    i = self._fields_map[k]
                    idx[rec[i]] = rec 
        except:
            raise
        finally:
            self._lock.release()

    def update(self, val, w={}):
        self._lock.acquire()
        try:
            keys = w.keys()
            # 对key排序，索引在前面
            indexkey = []
            noidxkey = []
            
            for i in xrange(0, len(keys)):
                k = keys[i]
                if not self._index_attr.has_key(k):
                    indexkey.append(k)
                else:
                    noidxkey.append(k)
            # 没有索引 
            if not indexkey:
                for rec in self._record:
                    isok = True
                    for k in w:
                        i = self._fields_map[k]
                        if rec[i] != w[k]:
                            isok = False
                            break
                    if isok:
                        for k in val:
                            i = self._fields_map[k]
                            if self._index_attr.has_key(k):
                                x = self._index[k]
                                if hasattr(x, "remove"):
                                    x.remove(rec[i], rec)
                                else:
                                    del x[rec[i]]
                                x[val[k]] = rec
                             
                            rec[i] = val[k]
            else: # 有索引
                res = []
                for k in indexkey:
                    idx = self._index[k]
                    v = idx[w[k]]
                    if not v:
                        return # return会释放finally里的锁吗
                    for x in res:
                        if x not in v:
                            res.remove(x)
                for k in noidxkey:
                    v = w[k]
                    i = self._fields_map[k]
                    for x in res:
                        if x[i] != v:
                            res.remove(x)
                            break
                # 所有需要更新的记录
                for rec in res:
                    for k in val:
                        i = self._fields_map[k]
                        if self._index_attr.has_key(k):
                            x = self._index[k]
                            if hasattr(x, "remove"):
                                x.remove(rec[i], rec)
                            else:
                                del x[rec[i]]
                            x[val[k]] = rec
 
                        rec[i] = val[k]
        except:
            raise
        finally:
            self._lock.release()
    
    def update_key(self, key, value, upval):
        '''通过一个索引来更新数据'''
        self._lock.acquire()
        try:
            idx = self._index[key]
            rec = idx[value]
            for r in rec:
                for k in upval:
                    i = self._fields_map[k]
                    # 更新的字段有索引，需要修改索引
                    if self._index_attr.has_key(k):
                        x = self._index[k]
                        if hasattr(x, "remove"):
                            x.remove(rec[i], rec)
                        else:
                            del x[rec[i]]
                        x[val[k]] = rec
                    rec[i] = val[k]
        except:
            raise
        finally:
            self._lock.release()

    def delete(self, w={}):
        '''遍历删除记录'''
        if not w:
            return self.clear()
        self._lock.acquire()
        try:
            dels = []
            rlen = len(self._record)
            for x in xrange(0, rlen):
                r = self._record[x]
                isok = True
                for k in w:
                    i = self._fields_map[k]
                    if r[i] != w[k]:
                        isok = False
                        break
                if isok:
                    dels.insert(0, x)

            # 删除时要删除索引和纪录
            for i in dels:
                for k in self._index_attr:
                    x = self._fields_map[k]
                    idx = self._index[k]
                    del idx[self._record[i][x]]
            # 删除会造成下标变动，可能会有问题, 这里是下表愈大越先删除
            for i in dels:
                del self._record[i]
        except: 
            raise
        finally:
            self._lock.release()
    
    def delete_key(self, key, value, upval):
        '''通过一个索引来删除数据'''
        self._lock.acquire()
        try:
            idx = self._index[key]
            rec = idx[value]
            for r in rec:
                for k in upval:
                    i = self._fields_map[k]
                    rec[i] = val[k]
        except:
            raise
        finally:
            self._lock.release()


    def pop(self, drt=0):
        '''弹出记录，drt是方向'''
        self._lock.acquire()
        try:
            if drt == 0:
                i = 0
            else:
                i = len(self._record) - 1
            # 删除该条记录的索引
            for k in self._index_attr:
                idx = self._index[k]
                x = self._fields_map[k]
                kv = self._record[i][x]
                #print 'kv:', kv
                v = idx[kv]
                if len(v) == 1:
                    del idx[kv]
                else:
                    if string.find(self._index_attr[k], 'u') >= 0:
                        del idx[kv]
                    else:
                        idx.remove(kv, self._record[i]) 
            ret = self._record[i]
            del self._record[i]
        except:
            raise
        finally:
            self._lock.release()
        return ret 

    def dump(self, filename, charset="utf-8", type="json"):
        '''把数据保存到文件,格式为 {'field': [xxx,xxx]'index': [index1,..], 'data':[xxx]}'''
        f = open(filename, "w")
        if type == 'json':
            import json
            obj = {}
            obj["field"] = self._fields
            obj["index"] = self._index_attr
            obj["data"]  = self._record

            f.write(json.write(obj)) 

        f.close()

    def load(self, filename, charset="utf-8", type="json"):
        f = open(filename, "r")
        if type == 'json':
            import json
            
            obj = json.read(f.read())
            
            self._fields = obj["field"]
            idxls = obj["index"]
            self._record = obj['data']
            
            for i in xrange(0, len(self._fields)):
                self._fields_map[self._fields[i]] = i

            for k in idxls:
                self._index[k] = {}

            for k in idxls:
                self.rebuild_index(k)

        f.close()

    
    def first(self):
        if len(self._record) > 0:
            #self._cur = 0
            return self._record[0]
        else:
            #self._cur = -1
            return None

            
    def __iter__(self):
        for x in self._record:
            yield x
        #return self

    #def next(self):
    #    rlen = len(self._record)
    #    if self._cur < rlen - 1:
    #        self._cur += 1
    #    else:
    #        self._cur = -1 
    #        raise StopIteration
    #    return self._record[self._cur]

    def last(self):
        rlen = len(self._record)
        if rlen > 0:
            #self._cur = rlen - 1
            #return self._record[self._cur]
            return self._record[-1]
        else:
            #self._cur = -1
            return None

    def add_index(self, field, attr='k'):
        '''添加索引 '''
        self._lock.acquire()
        try:
            if self._index_attr.has_key(field):
                return
            i = self._fields.index(field)
            self._index_attr[field] = attr
            idx = {}
            for r in self._record:
                idx[r[i]] = r
 
            self._index[field] = idx
        except:
            raise
        finally:
            self._lock.release()

    def remove_index(self, field):
        '''删除索引'''
        self._lock.acquire()
        try:
            del self._index[field]
            del self._index_attr[field]
        except:
            raise
        finally:
            self._lock.release()

    def rebuild_index(self, field):
        '''重建索引'''
        self._lock.acquire()
        try:
            i = self._fields_map[field]
            idx = self._index[field] 
            idx = {}
            for r in self._record:
                idx[r[i]] = r
        except:
            raise
        finally:
            self._lock.release()


def test():
    t = zTable(["name", "age", "sex"], {"name":"u", "age":"k"})

    t.insert(["zhaowei", 19, "f"])
    t.insert(["bobo", 18, "m"])
    t.insert(["maaibo", 16, "m"])
    t.insert(["bb", 26, "m"])
    t.insert(["zs", 29, "f"])
    t.insert(["mm", 16, "m"])
    t.insert(["gg", 28, "f"])
    t.insert(["haizi", 6, "f"])
    t.insert(["xxx", 35, "m"])

    print t
    print 'len:', len(t)
    print 'field name:', t.field_values('name')
    print 'index has key:', t.index_has_key('name', 'zhaowei')
    print 'select_adv name:', t.select_adv({"name": ('like', 'b')})
    print 'select_adv age:', t.select_adv({"age": ('>', 22)})
    print 'select_index:', t.select_index('name', 'zhaowei')
    
    print 'test iterator ...'
    for item in t:
        print item

    for k in t._index:
        print k, t._index[k]
    
    print 'select: name=zhaowei ', t.select({'name': 'zhaowei'})
    print 'update: age=20 where name=zhaowei'
    t.update({'age': 20}, {'name': 'zhaowei'})
    print t
    for k in t._index:
        print k, t._index[k]
    
    print 'delete: name=xxx '
    t.delete({'name': 'xxx'})
    print t
    
    print 'pop:'
    print 'pop:', t.pop()
    print t

    print 'first:', t.first()
    print 'last:', t.last()
    print t

    print t._index["name"].keys()
    print 'rebuild index name...'
    t.rebuild_index("name")
    print 'index:', t._index.keys()
    print 'name:', t._index["name"].keys()
    print t
    
    print 'remove index name'
    t.remove_index("name")
    print 'index:', t._index.keys()
    try:
        print 'name:', t._index["name"].keys()
    except Exception, e:
        print e
    print t
    
    print 'add index name '
    t.add_index("name")
    print 'index:', t._index.keys()
    print 'name:', t._index["name"].keys()
    print t
    
    t.reset()
    print 'iterator:'
    for x in t:
        print x
    print 'iterator:'
    for x in t:
        print x

    t.dump("data.txt")

def test2():
    t = zTable()
    t.load("data.txt")

    print t


def testdindex():
    d = MDict()

    d['a'] = 1
    d['b'] = 2
    d['c'] = 3
    print d
    d['a'] = 4
    d['a']= 5
    print d

    d.remove('a', 4)
    print d

if __name__ == '__main__':
    #test2()
    #testdindex()
    test()







