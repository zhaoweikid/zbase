# coding: utf-8
import string, os, sys, time, types
from bsddb import db
import cPickle as pickle
import struct
from zbase.base.logger import log

class DBTable:
    def __init__(self, homedir, tblname, fields, index=[], fieldstype=None):
        self.home = homedir
        self._fields = fields
        self._name = tblname
        self._index = index
        self._fields_hash = {}
        self.format = fieldstype

        if not os.path.isdir(self.home):
            os.mkdir(self.home)

        for i in xrange(0, len(fields)):
            self._fields_hash[fields[i]] = i
    
        self._env = db.DBEnv()
        self._env.open(self.home, 
                    db.DB_CREATE| db.DB_INIT_MPOOL| db.DB_INIT_LOG| db.DB_INIT_TXN| db.DB_RECOVER| db.DB_THREAD)

        self._primary_name = tblname + '.db'
        self._primary = db.DB(self._env) 
        #filename = os.path.join(self.home, self._primary_name)
        #filename = self._primary_name
        self._primary.open(self._primary_name, db.DB_BTREE, db.DB_CREATE|db.DB_THREAD, 0666)
        # secondary = {'index key' : [filename, db]}

        self._secondary = {}
        for x in index:
            filename = tblname + '.' + x + '.idx'
            sec = db.DB(self._env)
            sec.set_flags(db.DB_DUPSORT)
            sec.open(filename, db.DB_BTREE, db.DB_CREATE|db.DB_THREAD)
            self._secondary[x] = [filename, sec]
            idx = self._fields_hash[x]
            #self._primary.associate(sec, lambda a,b: pickle.loads(b)[idx])
            #self._primary.associate(sec, lambda a,b: struct.unpack(self.format, b)[idx])
            self._primary.associate(sec, lambda a,b: self._unpack(b)[idx])
        
        self._cursor = None

    def close(self):
        for k in self._secondary:
            x = self._secondary[k]
            sec = x[1]
            sec.sync()
            sec.close()
        
        self._primary.sync()
        self._primary.close()
        

    def sync(self):
        self._primary.sync()

    def _pack(self, *val):
        if not self.format:
            return pickle.dumps(val)
        else:
            return struct.pack(self.format, *val)

    def _unpack(self, bytes):
        if not self.format:
            return pickle.loads(bytes)
        else:
            return struct.unpack(self.format, bytes)


    def insert(self, values):
        t = type(values)
        if t == types.ListType or t == types.TupleType:
            #self._primary[values[0]] = pickle.dumps(values)
            #self._primary[str(values[0])] = struct.pack(self.format, *values)
            self._primary[str(values[0])] = self._pack(*values)
        elif t == types.DictType:
            v = []
            for k in self._fields:
                v.append(values[k])
            #self._primary[v[0]] = pickle.dumps(v)
            #self._primary[str(v[0])] = struct.pack(self.format, *v)
            self._primary[str(v[0])] = self._pack(*v)

    def insert_many(self, values):
        for row in values:
            self.insert(row)

        #self._primary.sync()

    def operation(self, row, cond):
        v  = row[cond[0]]
        op = cond[1]
        value = cond[2]

        if op == '=':
            return v == value
        elif op == '!=':
            return v != value
        elif op == 'prefix':
            return v.startswith(value)
        elif op == 'postfix':
            return v.endswith(value)
        elif op == 'like':
            return v.find(value) >= 0
        elif op == '>':
            return int(v) > int(value)
        elif op == '>=':
            return int(v) >= int(value)
        elif op == '<':
            return int(v) < int(value)
        elif op == '<=':
            return int(v) <= int(value)
        else:
            return False
        
    def find(self, key, val):
        pos = self._fields_hash[key]
        if key in self._secondary:
            retv = []
            cur = self._secondary[key][1].cursor()
            try:
                rec = cur.set(val)
                while rec:
                    #x = struct.unpack(self.format, rec[1])
                    x = self._unpack(rec[1])
                    if x[pos] == val:
                        retv.append(x)
                    rec = cur.next_dup()
            finally:
                cur.close()

            return retv
        else:
            if pos == 0:
                return [self._unpack(self._primary[val])]
            else:
                retv = []
                cur = self._primary.cursor()
                try:
                    rec = cur.first()
                    while rec:
                        #x = struct.unpack(self.format, rec[1])
                        x = self._unpack(rec[1])
                        if x[pos] == val:
                            retv.append(x)
                        rec = cur.next()
                finally:
                    cur.close()
                return retv

        

    def select(self, conditions=[]):
        '''conditions = [[index1, op, value1], [index2, op, value2]]
           fields: [k1, k2, k3] / [0, 3, 4]
           op: = > >= < <= != prefix postfix like
           eg: [[0, '=', '33'],]
        '''
        rows = []
        innercond = []
        for i in xrange(0, len(conditions)):
            item = conditions[i]
            innercond.append([self._fields_hash[item[0]], item[1], item[2]])

        cur = self._primary.cursor()
        rec = cur.first()
        while rec:
            #x = pickle.loads(rec[1])
            #x = struct.unpack(self.format, rec[1])
            x = self._unpack(rec[1])
            for cond in innercond:
                if self.operation(x, cond):
                    rows.append(x)
            if not innercond:
                rows.append(x)
            rec = cur.next()
        cur.close()

        return rows

    def select_dict(self, conditions=[]):
        '''return result by dictionary'''
        rows = self.select(conditions)

        myrows = []
        if rows:
            for row in rows:
                myrows.append(dict(zip(self._fields, row)))

        return myrows



    def update(self, values, conditions={}):
        '''values = [[key, val], ...]'''
        innercond = []
        for item in conditions:
            innercond.append([self._fields_hash[item[0]], item[1], item[2]])

        innerkey = []
        for item in values:
            innerkey.append([self._fields_hash[item[0]], item[1]])
        
        cur = self._primary.cursor()
        rec = cur.first()
        while rec:
            x = list(self._unpack(rec[1]))
            for cond in innercond:
               if self.operation(x, cond):
                    for item in innerkey:
                        x[item[0]] = item[1]
                    cur.put(rec[0], self._pack(*x), db.DB_CURRENT)
            rec = cur.next()
        cur.close()


    def delete(self, conditions={}):
        '''delete data by condition'''
        innercond = []
        for i in xrange(0, len(conditions)):
            item = conditions[i]
            innercond.append([self._fields_hash[item[0]], item[1], item[2]])

        cur = self._primary.cursor()
        rec = cur.first()
        while rec:
            #x = pickle.loads(rec[1])
            #x = struct.unpack(self.format, rec[1])
            x = self._unpack(rec[1])
            for cond in innercond:
                if self.operation(x, cond):
                    cur.delete()
            rec = cur.next()
        cur.close()

 
   
    def get(self, pair):
        '''pair = [k, v]'''
        if type(pair) == types.ListType:
            return self.find(pair[0], pair[1])
        elif type(pair) == types.StringType:
            return self.find(self._fields[0], pair)
        
    def get_primary_db(self):
        return self._primary

    def get_secondary_db(self, index):
        return self._secondary[index][1]

    def delete_key(self, pair):
        '''delete data by index'''
        if type(pair) == types.ListType:
            k = pair[0]
            v = pair[1]
            
            pos = self._fields_hash[k]
            if pos == 0:
                self._primary.delete(v)
                return
            indexdb = self._secondary[k][1]
            indexdb.delete(v)
            return
        elif type(pair) == types.StringType:
            self._primary.delete(pair)
            return
             
    '''note: methods below not thread safe'''
    def reset(self):
        if self._cursor:
            self._cursor.close()
        self._cursor = self._primary.cursor()
    
    def first(self):
        return self._cursor.first()

    def last(self):
        return self._cursor.last()

    def next(self):
        return self._cursor.next()

    
def test():
    tbl = DBTable("dbhome", "testdb", ['id', 'name', 'age', 'sex'], ['name'])
    tbl.insert(['1', 'zhaowie', '12', 'fa'])
    tbl.insert(['2', 'zhaowei', '18', 'm'])
    print 'get 1:', tbl.get('1')
    print 'select all:', tbl.select()
    print 'select 1:', tbl.select([['id', '=', '1']])
    tbl.update([['name','bobo']], [['id', '=', '2']])
    print 'after update select all:', tbl.select()
    tbl.delete([['id', '=', '1']])
    print 'after delete select all:', tbl.select()
    tbl.delete_key(['name','bobo'])
    print 'after delete_key select all:', tbl.select()
    tbl.close()

def test2():
    tbl = DBTable("dbhome", "testdb", ['id', 'name', 'age', 'sex'], ['name'])
    start = time.time()
    for i in xrange(0, 100000):
        tbl.insert([str(i), 'zhaowei'+str(i), str(i+10), 'm'])
    end = time.time()
    print 'insert 100000 time:', end-start
    
    start = time.time()
    for i in xrange(0, 100000):
        tbl.get(str(i))
    end = time.time()
    print 'get 100000 time:', end-start

    start = time.time()
    for i in xrange(0, 100):
        tbl.select([['id', '=', str(i)]])
    end = time.time()
    print 'select 100 time:', end-start


    tbl.close()

def test3():
    import pprint

    tbl1 = DBTable("dbhome", "testdb", ['id', 'name', 'age', 'sex'], ['name'], 's5s3s2s')
    
    for i in range(0, 10):
        tbl1.insert([str(i), 'bobo'+str(i%2), '222', 'ff'])

    #tbl1.insert(['1', 'bobo', '12', 'fa'])
    #tbl1.insert(['2', 'zhaowei', '18', 'm'])
    tbl1.sync()

    print 'tbl1 select all:', 
    pprint.pprint(tbl1.select())
    print 'tbl1 select all:', 
    pprint.pprint(tbl1.select_dict())
    print 'tbl1 find bobo1:', 
    pprint.pprint(tbl1.find('name', 'bobo1'))
    print 'tbl1 find id2:', 
    pprint.pprint(tbl1.find('id', '2'))
    print 'tbl1 find age 222:', 
    pprint.pprint(tbl1.find('age', '222'))



    #tbl2 = DBTable("dbhome", "testdb", ['id', 'name', 'age', 'sex'], ['name'])

    #print 'tbl2 select all:', 
    #pprint.pprint(tbl2.select())
    #print 'tbl2 select all:', 
    #pprint.pprint(tbl2.select_dict())

    tbl1.close()
    #tbl2.close()


if __name__ == '__main__':
    test3()

