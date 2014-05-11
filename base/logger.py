# coding: utf-8
import os, sys
import types
import logging
import logging.config
from logging import INFO,DEBUG,INFO,WARN,ERROR,FATAL,NOTSET

LEVEL_COLOR = {DEBUG:'\33[39m', 
               INFO:'\33[39m', 
               WARN:'\33[33m',
               ERROR:'\33[35m', 
               FATAL:'\33[31m', 
               NOTSET:''}

log = None

class ScreenHandler (logging.StreamHandler):
    def emit(self, record):
        try: 
            msg = self.format(record)
            stream = self.stream
            fs = LEVEL_COLOR[record.levelno] + "%s\n" + '\33[0m'
            if not logging._unicode: #if no unicode support...
                stream.write(fs % msg) 
            else:
                try: 
                    if (isinstance(msg, unicode) and
                        getattr(stream, 'encoding', None)):
                        ufs = fs.decode(stream.encoding)
                        try:
                            stream.write(ufs % msg)
                        except UnicodeEncodeError:
                            stream.write((ufs % msg).encode(stream.encoding))
                    else:
                        stream.write(fs % msg)
                except UnicodeError:
                    stream.write(fs % msg.encode("UTF-8"))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except: 
            self.handleError(record)

logging.ScreenHandler = ScreenHandler

def debug(msg, *args, **kwargs):
    global log
    log.debug(msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    global log
    log.info(msg, *args, **kwargs)
note = info

def warn(msg, *args, **kwargs):
    global log
    log.warn(msg, *args, **kwargs)
warning = warn

def error(msg, *args, **kwargs):
    global log
    log.error(msg, *args, **kwargs)

def fatal(msg, *args, **kwargs):
    global log
    log.fatal(msg, *args, **kwargs)

critical = fatal

def install(filename='stdout', maxBytes=1024000000, backupCount=10):
    global log
    tfilename_str = type(filename) in (types.StringType,types.UnicodeType)
    pyv = sys.version_info
    if pyv[0] == 2 and pyv[1] < 7:
        if not tfilename_str:
            print 'python error, must python >= 2.7'
            return
        if tfilename_str and filename == 'stdout':
            filename = None
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s',
                            #datefmt='%Y%m%d %H:%M:%S',
                            filename=filename,
                            filemode='w')
        log = logging.getLogger()
        log.note = log.info
        return

    conf = {
        'version': 1,
        'formatters': {
            'myformat': {
                'format': '%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.ScreenHandler',
                'formatter': 'myformat',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            },
        },
        'loggers': {
            #'zbase': {
            #    'level': 'DEBUG',
            #    'handlers': ['console'],
            #},
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console'],
        },

    } 

    
    if tfilename_str and filename != 'stdout':
        filecf = {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'myformat',
            'level': 'DEBUG',
            'filename': filename,
            'maxBytes': maxBytes,
            'backupCount': backupCount,
        }

        conf['handlers']['file'] = filecf
        #conf['loggers']['zbase']['handlers'] = ['file']
        conf['root']['handlers'] = ['file']

    elif not tfilename_str: # filename: {'DEBUG':"test.log", "WARN":"test.log"}
        filehandlers = []
        for level,name in filename.iteritems():
            filecf = {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'myformat',
                'level': level,
                'filename': name,
                'maxBytes': maxBytes,
                'backupCount': backupCount,
            }
            conf['handlers']['file-'+name] = filecf
            filehandlers.append('file-'+name)
        conf['root']['handlers'] = filehandlers

    logging.config.dictConfig(conf)
    log = logging.getLogger()
    return log

def install_dict(conf):
    logging.config.dictConfig(conf)
    global log
    key = conf['loggers'].keys()[0]
    log = logging.getLogger(key)
    return log


install()

def test():
    def loginit():
        logger  = logging.getLogger()
        #handler = logging.StreamHandler()
        handler = ScreenHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.setLevel(logging.NOTSET)
        return logger

    log = loginit()

    for i in range(0, 10):
        log.info('info ...')
        log.debug('debug ...')
        log.warn('warn ...')
        log.error('error ...')
        log.fatal('fatal ...')

def test2():
    conf = {
        'version': 1,
        'formatters': {
            'myformat': {
                'format': '%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s',
            },
        },
        'handlers': {
            'myhandler': {
                'class': 'logging.ScreenHandler',
                'formatter': 'myformat',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            },

        },
        'loggers': {
            'test': {
                'level': 'DEBUG',
                'handlers': ['myhandler'],
            },
        },
    } 
    logging.config.dictConfig(conf)
    log = logging.getLogger('test')

    for i in range(0, 10):
        log.debug('debug ...')
        log.info('info ...')
        log.warn('warn ...')
        log.error('error ...')
        log.fatal('fatal ...')

def test3():
    conf = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'myformat': {
                'format': '%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.ScreenHandler',
                'formatter': 'myformat',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'myformat',
                'level': 'DEBUG',
                'filename': 'test.log',
            },

        },
        'loggers': {
            'test': {
                'level': 'DEBUG',
                'handlers': ['console'],
            },
        },
    } 
    logging.config.dictConfig(conf)
    log = logging.getLogger('test')

    for i in range(0, 10):
        log.debug('debug ...')
        log.info('info ...')
        log.warn('warn ...')
        log.error('error ...')
        log.fatal('fatal ...')

def test4():
    for i in range(0, 10):
        log.debug('debug ...')
        log.info('info ...')
        log.warn('warn ...')
        log.error('error ...')
        log.fatal('fatal ...')

def test5():
    #log = logging.getLogger('zbase')

    for i in range(0, 10):
        logging.debug('debug ... %d', i)
        logging.info('info ... %d', i)
        logging.warn('warn ... %d', i)
        logging.error('error ... %d', i)
        logging.fatal('fatal ... %d', i)


def test6():
    install({"DEBUG":"test.log", "WARN":"test-warn.log", "ERROR":"test-err.log"})

    for i in range(0, 10):
        logging.debug('debug ... %d', i)
        logging.info('info ... %d', i)
        logging.warn('warn ... %d', i)
        logging.error('error ... %d', i)
        logging.fatal('fatal ... %d', i)


if __name__ == '__main__':
    test6()

