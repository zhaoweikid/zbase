# coding: utf-8
import sys, os
import json
import log

plugins = {}

def load_plugins(plgs):
    global plugins
    for plugin in plgs:
        m = __import__(plugin, None, None, [''])
        plugins[plugin] = m

def load_plugin(plugin):
    global plugins
    log.info('load plugin: %s' %  str(plugin))
    m = __import__(plugin, None, None, [''])
    plugins[plugin] = m

def init_plugin_system(plgpath):
    plgpath = os.path.abspath(plgpath)
    if not os.path.isdir(plgpath):
        raise IOError, 'Not a directory'
        return
    if not plgpath in sys.path:
        sys.path.insert(0, plgpath)
    for x in os.listdir(plgpath):
        ext = os.path.splitext(x)
        checkfile = plgpath + os.sep + x
        if os.path.isfile(checkfile) and ext[1] == '.py':
            module = x[:-3]
            try:
                load_plugin(module)
            except Exception, e:
                log.error('load plugin %s error! %s' % (module, str(e)))

def MixIn(mainClass, mixInClass):
    log.info("Mix class:",mixInClass, " into: ",mainClass)
    mainClass.__bases__ += (mixInClass,)


if __name__ == '__main__':
    init_plugin_system(sys.argv[1])

    print plugins
    
