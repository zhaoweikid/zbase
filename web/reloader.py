# coding: utf-8
# reloader from web.py

import os, sys
import traceback
import logging

log = logging.getLogger()

class Reloader:
    """Checks to see if any loaded modules have changed on disk and, 
    if so, reloads them.
    """
    SUFFIX = '.pyc'

    def __init__(self):
        self.mtimes = {}

    def __call__(self):
        for mod in sys.modules.values():
            self.check(mod)

    def check(self, mod):
        # jython registers java packages as modules but they either
        # don't have a __file__ attribute or its value is None
        if not (mod and hasattr(mod, '__file__') and mod.__file__):
            return
        try: 
            mtime = os.stat(mod.__file__).st_mtime
        except (OSError, IOError):
            return
        if mod.__file__.endswith(self.__class__.SUFFIX) and os.path.exists(mod.__file__[:-1]):
            mtime = max(os.stat(mod.__file__[:-1]).st_mtime, mtime)
    
        if mod not in self.mtimes:
            self.mtimes[mod] = mtime
        elif self.mtimes[mod] < mtime:
            try: 
                log.debug('reload %s', mod)
                reload(mod)
                self.mtimes[mod] = mtime
            except ImportError: 
                log.debug('reload error: %s', traceback.format_exc())
        
    def __str__(self):
        s = []
        for k,v in self.mtimes.iteritems():
            fname = k.__file__
            if not fname.startswith('/usr') and fname.find('/python') < 0 and fname.find('zbase') < 0:
                s.append('%s=%d'% (k, v))
        return '\n'.join(s)

class ProcReloader:
    def __init__(self):
        pass

      








