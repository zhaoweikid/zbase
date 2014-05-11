# coding: utf-8
import os, string, sys, time
import types, locale, traceback
import threading
import logging


INFO    = 1
DEBUG   = 2
NOTE    = 3
WARN    = 4
ERROR   = 5
FATAL   = 6
NOLOG   = 10

ROTATE_NO     = 0
ROTATE_SIZE   = 1
ROTATE_TIME   = 2
ROTATE_TIMEAT = 3

LEVEL_STR = {INFO:'info', DEBUG:'debug', NOTE:'note',WARN:'warn',
             ERROR:'error', FATAL:'fatal', NOLOG:'nolog'}
LEVEL_COLOR = {INFO:'\33[37m', DEBUG:'\33[39m', NOTE:'\33[36m',WARN:'\33[32m',
               ERROR:'\33[35m', FATAL:'\33[31m', NOLOG:''}

FORMAT_PRINT    = 1
FORMAT_STRING   = 2

class Logger:
    charset = 'utf-8'
    end     = '\n'
    level   = 0
    rotate_type  = ROTATE_NO
    format_style = FORMAT_PRINT
    flush   = False
    
    def __init__(self):
        self.prefix  = {}
        x = locale.getdefaultlocale()
        #self.info("Logger report system locale:", x)
        if sys.platform.startswith('darwin'):
            try:
                self.charset = os.environ['LANG'].split('.')[1].lower()
            except:
                self.charset = 'utf-8'
        else:
            if x and x[1]:
                self.charset = x[1]

    def set_prefix(self, s):
        self.prefix[threading.currentThread().getName()] = s

    def _format_str(self, *s):
        if self.format_style == FORMAT_PRINT:
            s = list(s)
            for k in xrange(0, len(s)):
                v = s[k]
                if type(v) == types.UnicodeType:
                    s[k] =  v.encode(self.charset)
                elif type(v) != types.StringType:
                    s[k] = str(v)
                else:
                    s[k] = str(v)

            return ' '.join(s)
        else:
            return s[0] % tuple(s[1:])

    def _format(self, level='inf', *s):
        prefix = self.prefix.get(threading.currentThread().getName(), '')
        if self.format_style == FORMAT_PRINT:
            s = list(s)
            for k in xrange(0, len(s)):
                v = s[k]
                if type(v) == types.UnicodeType:
                    s[k] =  v.encode(self.charset)
                elif type(v) != types.StringType:
                    s[k] = str(v)
                else:
                    s[k] = str(v)

            infos = traceback.extract_stack()[-4]
            ifs = string.split(infos[0], os.sep)
            filename = ifs[-1]
            line = infos[1]

            ss = '%d%02d%02d %02d:%02d:%02d' % time.localtime()[:6]
            ss += ',%.f %s,%s %s:%d [%s] %s %s%s' % (time.time()%1*1000,
                    os.getpid(), threading.currentThread().getName(), 
                    filename, line, level, prefix, ' '.join(s), self.end)
        else:
            s1 = s[0] % tuple(s[1:])
            infos = traceback.extract_stack()[-4]
            ifs = string.split(infos[0], os.sep)
            filename = ifs[-1]
            line = infos[1]

            ss = '%d%02d%02d %02d:%02d:%02d' % time.localtime()[:6]
            ss += ',%.f %s,%s %s:%d [%s] %s %s%s' % (time.time()%1*1000,
                    os.getpid(), threading.currentThread().getName(), 
                    filename, line, level, prefix, s1, self.end)
 
        return ss

    def set_level(self, lev):
        if type(lev) == types.StringType:
            lev = globals()[lev]
        self.level = lev

    def write(self, s, level=INFO):
        pass

    def dolog(self, level, *s):
        if self.level <= level:
            self.write(self._format(LEVEL_STR[level], *s), level)

    def close(self):
        pass


class DummyLogger (Logger):
    def __init__(self):
        #sys.stderr.write('=== DummyLogger! ===\n')
        pass

    def dolog(self, level, *s):
        pass

logobj  = DummyLogger()


class SimpleLogger (Logger):
    def __init__(self, end='\n'):
        sys.stderr.write('=== SimpleLogger! ===\n')
        Logger.__init__(self)

    def dolog(self, level, *s):
        prefix = self.prefix.get(threading.currentThread().getName(), '')
        if self.level <= level:
            print '%d%02d%02d %02d:%02d:%02d,%03d' % time.localtime()[:7], '[%s]' % LEVEL_STR[level], prefix, 
            for x in s:
                if type(x) == types.UnicodeType:
                    print x.encode(self.charset),
                else:
                    print x, 
            print

class ScreenLogger (Logger):
    def __init__(self, end='\n'):
        sys.stderr.write('=== ScreenLogger! ===\n')
        Logger.__init__(self)

    def write(self, s, level=INFO):
        ss = '%s%s\33[0m' % (LEVEL_COLOR[level], s.rstrip())
        print ss



class FileLogger (Logger):
    def __init__(self, name, maxsize=0, maxnum=0, end='\n'):
        sys.stderr.write('=== FileLogger! ===\n')
        if not name:
            raise LogError, 'logfile name error!'

        self.logfile = os.path.abspath(name)
        self.log = None

        self.rotate_size = maxsize
        self.rotate_time = None
        self.rotate_timeat = None
        self.logcount = maxnum
        self.lasttime = time.time()
        self.last_rotate_time = 0
        self.rotate_interval = 10

        self.end = end
        self.lock = threading.Lock()
        self.open()

        Logger.__init__(self)

    def set_rotate_size(self, maxsize, maxnum=0):
        self.rotate_type = ROTATE_SIZE
        self.rotate_size = maxsize
        self.logcount = maxnum

    def set_rotate_no(self):
        self.rotate_type = ROTATE_NO

    def set_rotate_time(self, tm, maxnum=0):
        self.rotate_type = ROTATE_TIME
        self.rotate_time = tm
        self.logcount = maxnum

    def set_rotate_timeat(self, tm, maxnum=0):
        self.rotate_type = ROTATE_TIMEAT
        self.rotate_timeat = tm
        self.logcount = maxnum

    def open(self):
        self.log = open(self.logfile, 'a+')
        
    def close(self):
        self.log.flush()
        if self.log:
            self.log.close()
            self.log = None

    def rotate(self):
        newname = self.logfile + '.%04d%02d%02d.%02d%02d%02d' % time.localtime()[:6]
        self.log.close()
        #print 'rename ', self.logfile, newname
        os.rename(self.logfile, newname)
        self.last_rotate_time = time.time()
        #self.lasttime = time.time()
        self.open()
    
    def write(self, s, level=INFO):
        timenow = time.time()
        tmdiff = timenow - self.lasttime
        if self.logfile and tmdiff > self.rotate_interval:
            if self.rotate_type == ROTATE_NO:
                self.lock.acquire()
                try:
                    self.log.close()
                    self.open()
                finally:
                    self.lock.release()
            elif self.rotate_type == ROTATE_SIZE:
                size = os.path.getsize(self.logfile)
                
                if self.rotate_size > 0 and size > self.rotate_size:
                    self.lock.acquire()
                    try:
                        self.rotate()
                    except Exception, e:
                        traceback.print_exc()
                    self.lock.release()
                self.lasttime = timenow
            elif self.rotate_type == ROTATE_TIME:
                pass
            elif self.rotate_type == ROTATE_TIMEAT:
                pass

        self.log.write(s)
        if self.flush:
            self.log.flush()

class PythonLogger (Logger):
    def __init__(self, filename=None):
        Logger.__init__(self)

        self.logger = logging.getLogger()
        if filename:
            hdlr = handlers.RotatingFileHandler(filename, maxBytes=100000000, backupCount=10)
        else:
            hdlr = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(threadName)s %(filename)s:%(lineno)d %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        
        self.set_level(self.level)

    def set_level(self, lev):
        if type(lev) == types.StringType:
            lev = globals()[lev]

        if lev == INFO:
            lev == logging.INFO
        elif lev == WARN:
            lev == logging.WARN
        elif lev == ERROR:
            lev == logging.ERROR
        else:
            lev == logging.INFO
        self.logger.setLevel(lev)

    def dolog(self, level, *s):
        if level == INFO:
            return self.logger.info(self._format_str(*s))
        if level == WARN:
            return self.logger.warning(self._format_str(*s))
        if level == ERROR:
            return self.logger.error(self._format_str(*s))
        return self.logger.info(self._format_str(*s))

    
if not sys.platform.startswith('win'):
    import syslog
    class SysLogger (Logger):
        def __init__(self, name, end='\n'):
            sys.stderr.write('=== SysLogger! ===\n')
            syslog.openlog(name, syslog.LOG_PID, syslog.LOG_USER)
            self.end = end

            self.levelmap = {INFO:syslog.LOG_INFO, DEBUG:syslog.LOG_DEBUG, 
                             NOTE:syslog.LOG_NOTICE, WARN:syslog.LOG_WARNING, 
                             ERROR:syslog.LOG_ERR, FATAL:syslog.LOG_CRIT}

            Logger.__init__(self)
    
        def _format(self, level='info', *s):
            if self.format_style == FORMAT_PRINT:
                s = list(s)
                for k in xrange(0, len(s)):
                    v = s[k]
                    if type(v) == types.UnicodeType:
                        s[k] =  v.encode(self.charset)
                    elif type(v) != types.StringType:
                        s[k] = str(v)
                    else:
                        s[k] = str(v)
                
                infos = traceback.extract_stack()[-4]
                ifs = string.split(infos[0], os.sep)
                filename = ifs[-1]
                line = infos[1]

                ss = '%s %s:%d %s%s' % (threading.currentThread().getName(), filename, line, ' '.join(s), self.end)
            else:
                s1 = s[0] % tuple(s[1:])
                infos = traceback.extract_stack()[-4]
                ifs = string.split(infos[0], os.sep)
                filename = ifs[-1]
                line = infos[1]
                
                ss = '%s %s:%d %s%s' % (threading.currentThread().getName(), filename, line, s1, self.end)
            return ss


        def write(self, s, level=INFO):
            flags = self.levelmap.get(level, syslog.LOG_INFO)
            ss = '[%s] %s' % (LEVEL_STR[level], s)
            syslog.syslog(flags, ss)

def install_pythonlog(filename=None):
    PythonLogger(filename)

def install_screenlog():
    global logobj
    if logobj:
        logobj.close()
    logobj = ScreenLogger()
   
def install_filelog(name, size=10240000, num=10):
    global logobj
    if logobj:
        logobj.close()
    logobj = FileLogger(name, maxsize=maxsize, maxnum=maxnum)

def install_syslog():
    global logobj
    if logobj:
        logobj.close()
    logobj = SysLogger("testlog")

def install(classname, *args, **kwargs):
    if type(classname) == types.StringType:
        classname = globals()[classname]
    global logobj
    if logobj:
        logobj.close()

    lev = kwargs.get('level')
    if lev:
        del kwargs['level']
    logobj = classname(*args, **kwargs)
    if lev:
        logobj.set_level(lev)
    
    return logobj

def info(*s):
    global logobj
    logobj.dolog(INFO, *s)

def debug(*s):
    global logobj
    logobj.dolog(DEBUG, *s)

def note(*s):
    global logobj
    logobj.dolog(NOTE, *s)

def warn(*s):
    global logobj
    logobj.dolog(WARN, *s)

warning = warn

def err(*s):
    global logobj
    logobj.dolog(ERROR, *s)

error = err

def fatal(*s):
    global logobj
    logobj.dolog(FATAL, *s)

def test():
    #syslog_init()
    #filelog_init()
    #screenlog_init()
    
    #install(PythonLogger)
    #log = install(SimpleLogger)
    install("ScreenLogger")
    #install("PythonLogger")
    #install("SysLogger", name='zw')
    #install("SimpleLogger")
    #install("FileLogger", name="test.log", maxsize=1024, maxnum=10)
    
    for x in xrange(0, 1):
        for i in xrange(0, 100):
            #loginfo('hehe, loginfo...')
            #logwarn('hehe, logwarn...')
            #logerr('hehe, logerr...')
            logobj.set_prefix('<%d>' % i)
            info('hehe, loginfo...', i)
            debug('hehe, logdebug...', i)
            note('hehe, lognote...', i)
            warn('hehe, logwarn...', i)
            err('hehe, logerr...', i)
            fatal('hehe, logfatal...', i)

if __name__ == '__main__':
    test()



