# coding: utf-8
import os, sys
import signal, time
import traceback
import logging

log = logging.getLogger()

class Watcher:
    def __init__(self):
        self.watches = []
        self.watches_running = {}
        self.children = {}
        self.running = False

    def add(self, func):
        self.watches.append(func)

    def start(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        signal.signal(signal.SIGCHLD, self.child_handler)
        signal.signal(signal.SIGTERM, self.term_handler) 

        for func in self.watches:
            self.child_start(func)

        self.running = True

    def child_start(self, func):
        log.info('start', func.__name__)
        pid = os.fork()
        if pid == 0: # child
            signal.signal(signal.SIGTERM, signal.SIG_DFL) 
            signal.signal(signal.SIGCHLD, signal.SIG_DFL) 
            func()
            sys.exit(0)
        elif pid > 0: # parent
            #self.watches_running[func.__name__] = {'pid':pid, 'run':func}
            self.watches_running[pid] = {'pid':pid, 'run':func}
        else:
            log.err('fork err:', pid)


    def child_handler(self, signo, frame):
        #log.info('child signal:', signo)
        while self.running:
            try:
                pid, status = os.wait()
            except:
                break
            log.info('wait pid:', pid)
            x = self.watches_running.get(pid)
            if not x:
                log.info('not found pid:', pid)
                return
            del self.watches_running[pid]
            self.child_start(x['run'])

    def term_handler(self, signo, frame):
        #log.info('child signal:', signo)
        for k,v in self.watches_running.iteritems():
            log.info('kill %d' % (v['pid']))
            self.running = False
            os.kill(v['pid'], signal.SIGTERM)
        os.kill(os.getpid(), signal.SIGKILL)

def test():
    def test1():
        while True:
            log.info('test1 haha')
            time.sleep(1)

    log.install('ScreenLogger')
    wt = Watcher()
    wt.add(test1)
    wt.start()

    while True:
        time.sleep(1)

if __name__ == '__main__':
    test()



