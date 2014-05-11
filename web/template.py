# coding: utf-8
import os, sys
import glob
import shutil
import traceback
from mako.template import Template
from mako.lookup import TemplateLookup
from mako import exceptions
import zbase
from zbase.base.logger import log

render = None

class Render:
    def __init__(self, loc="templates", tmpdir=None, cache=False, charset='utf-8'):
        self.loc = loc
        if cache:
            self.cache = {}
        else:
            self.cache = False

        self.charset = charset

        if tmpdir:
            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            # rm old data
            exidirs = glob.glob(tmpdir + "/flying*")
            for d in exidirs:
                shutil.rmtree(d)
            self.tmpdir = os.path.join(tmpdir, "flying."+str(os.getpid()))
        else:
            self.tmpdir = None
        
    def __call__(self, tplname, **args):
        return self.display(tplname, **args)

    def display(self, tplname, **args):
        try:
            if not tplname.startswith(self.loc):
                tplname = os.path.join(self.loc, tplname)

            if self.cache is False or tplname not in self.cache:
                mylookup = TemplateLookup(directories=[self.loc], 
                                          filesystem_checks=True, 
                                          module_directory=self.tmpdir, 
                                          output_encoding=self.charset, 
                                          encoding_errors='replace', 
                                          default_filters=['unicode'])
                c = Template(filename=tplname, lookup=mylookup, 
                             output_encoding=self.charset, 
                             encoding_errors='ignore')

                if self.cache is not False:
                    self.cache[tplname] = c
                        
            s = c.render(**args)

            if self.cache:
                c = self.cache[tplname]

            return s
        except:
            log.error('\n=== template error ===\n')
            log.error(exceptions.text_error_template().render())
            log.error('=== template end ===')
            return 'template error!'
    
    #def display2(self, tplname, **args):
    #    if self.cache is False or tplname not in self.cache:
    #        fpath = os.path.join(self.loc, tplname)
    #        f = open(fpath, 'r')
    #        s = f.read()
    #        f.close()
    #    
    #        c = Template(s, output_encoding=self.charset, 
    #                     encoding_errors='ignore')
    #        if self.cache is not False:
    #            self.cache[tplname] = c
    #    if self.cache:
    #        c = self.cache[tplname]
    #        
    #    return c.render(**args)
    #    
        
    
def install(loc="templates", tmpdir="/tmp", cache=False, charset='utf-8'):
    global render
    render = Render(loc, tmpdir, cache, charset)
    return render
 

def with_template(tplfile):
    def f(func):
        def _(self, *args, **kwargs):
            global render
            x = func(self, *args, **kwargs)
            return render.display(tplfile, **x) 
        return _
    return f

def test():
    zbase.base.logger.install()
    #loc = os.path.join(path, 'templates')
    loc = '/Users/apple/projects/python/xx8xx8/web/templates/default/admin'
    r = Render(loc, 'tmp')
    print r.display("test.html", name='zhaowei')
  
if __name__ == '__main__':
    test()
    
        
    
    
