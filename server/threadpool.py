# coding: utf-8
import string, sys, os, time
import threading
import Queue, traceback
import log

TASK_NORET = 0
TASK_RET   = 1
TASK_NOTIFY_RET = 2

class ThreadPool:
    def __init__(self, num):
        self.queue   = Queue.Queue()
        self.result  = {} 
        self.threads = []
        self.count = num
        self.isrunning = False
        self.mutex = threading.Lock() 
        self.task_done  = 0
        self.task_error = 0
        # 正在执行任务的线程数
        self.thread_running = 0

    def start(self):
        # 如果标记为已经在运行就不能再创建新的线程池运行了
        if self.isrunning:
            return
        for i in range(0, self.count):
            t = threading.Thread(target=self._run)
            self.threads.append(t)
            t.setDaemon(True)
        
        self.isrunning = True
        for th in self.threads:
            th.start()

    def stop(self):
        self.isrunning = False
        # 等待其他线程退出
        while True:
            #self.mutex.acquire()
            #tr = self.thread_running
            #self.mutex.release()
            #if tr == 0:
            if self.thread_running == 0:
                break
            time.sleep(1)
        
    def _run(self):
        while True:
            task = None
            while True:
                if not self.isrunning:
                    log.info('stop!')
                    return
                try:
                    task = self.queue.get(timeout=1)
                except Exception, e:
                    #log.info('get timeout, self.queue.get:',  str(e))
                    continue
                break
            self.do_task(task)


    def do_task(self, task):
        if not task:
            log.err('get task none: %s' % (task.name))
            return

        log.info('get task: %s' % (task.name))
        self.thread_running += 1

        try:
            ret = task.run()
        except Exception, e:
            log.err('task %s run error: %s' % (task.name, str(e)))
            #traceback.print_exc(file=sys.stdout)
            log.err(traceback.format_exc())
            self.thread_running -= 1
            self.task_error += 1
            return
        
        self.task_done += 1
        self.thread_running -= 1

        task.setret(ret)

        log.info('task %s run complete' % task.name)

    def add(self, task):
        self.queue.put(task) 

    def info(self):
        return (self.task_done, self.task_error)


class Task(object):
    def __init__(self, retval=False, timeout=1):
        self.name = self.__class__.__name__
        #self.args = args
        self._timeout = timeout
        self._retval  = retval
        self._result  = None
        #self._notify  = notify
        #self._event   = None #threading.Event()

        #if notify:
        #    self._event = threading.Event()
        self._event = threading.Event()

    def run(self):
        pass

    def setret(self, result):
        if not self._retval:
            return
        self._result = result
        self.notify()

    def getret(self, timeout=1):
        try:
            self._event.wait(timeout)
        except:
            return None
        return self._result

class SimpleTask(Task):
    def __init__(self, n, a=None):
        self.name = n
        super(SimpleTask, self).__init__(a)
    
    def run(self):
        #log.info('in task run, ', self.name)
        time.sleep(1)
        #log.info('ok, end task run', self.name)

        return self.name

def test():
    log.install('ScreenLogger')
    tp = ThreadPool(10)

    for i in range(0, 100):
        t = SimpleTask(str(i))
        tp.add(t)
    
    tp.start()
    while True: 
        done, error = tp.info()
        log.info('applys:', done, error)
        cc = done + error
        time.sleep(1)
        if cc == 100:
            break
    tp.stop()
    log.info('end')

        
if __name__ == '__main__':
    test()



