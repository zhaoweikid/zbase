# coding: utf-8
import os,sys
import traceback
import zbase

def walk():
    fn = sys.modules['zbase'].__file__
    if fn.endswith(('.py', '.pyc')):
        fn = os.path.dirname(fn)
    print fn

    check_files = []
    for root,dirs,files in os.walk(fn):
        for f in files:
            if f.startswith(('.','_')):
                continue
            if '/thriftclient/' in root:
                continue
            if '/test' in root:
                continue
            if '/web/project/' in root:
                continue

            if f.endswith('.py'):
                #print f
                name = os.path.join(root, f)[len(fn):]
                check_files.append('zbase'+name.replace('/','.')[:-3])

        
    print check_files

    result = []
    for ckm in check_files:
        print '='*10, ckm
        try:
            __import__(ckm)
            test_funcs = []
            m = zbase
            for a in ckm.split('.')[1:]:
                m = getattr(m, a)

            fs = dir(m)
            for x in fs:
                if x.startswith('test'):
                    test_funcs.append(x)

            for func in test_funcs:
                print 'run:', func
                f = getattr(m, func)
                try:
                    f()
                except Exception, e:
                    ret = {'module':ckm, 'func':func, 'ret':1, 'err':traceback.format_exc()}
                    result.append(ret)
                else:
                    result.append({'module':ckm, 'func':func, 'ret':0, 'err':''})
        except:
            traceback.print_exc()

    msg_map_color = {0:'\33[2;32msucc\33[0m', 1:'\33[2;41mfail\33[0m'}
    msg_map = {0:'succ', 1:'fail'}

    for x in result:
        name = '%s: %s' % (x['module'], x['func'])
        print '%s\t%s' % (msg_map_color[x['ret']], name)
    print 'detail in result.log'
    
    with open('result.log', 'w') as f:
        for x in result:
            name = '%s: %s' % (x['module'], x['func'])
            ret = '%s\t%s\t%s\n' % (name, msg_map[x['ret']], x['err'])
            f.write(ret)
            



if __name__ == '__main__':
    walk()


