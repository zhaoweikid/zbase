# coding: utf-8
import os, sys, string
import cPickle, cStringIO

class ClientCookie:
    def __init__(self):
        self.clear()

    def clear(self):
        self._cookie = {}
        self.default_item = [
            "commenturl", 
            "discard",
            "domain",
            "max-age",
            "path",
            "port",
            "secure",
            "version",
            "httponly",
            "expires"
            ]
        self._default = {}
    
    def load_dump(self, data=None):
        self._cookie = cPickle.load(data)
    
    def load_file(self, filename):
        f = open(filename, 'rb')
        self._cookie = cPickle.load(f.read())
        f.close()
        
    def _parse(self, data):
        if not data:
            return
        cmdsp = string.split(data, ":", 1)
        if len(cmdsp) == 2:
            if cmdsp[0].strip().lower().startswith('set-cookie'):
                attrs = string.strip(cmdsp[1])
            else:
                attrs = data.strip()
        else:
            attrs = data.strip()

        attrsp = string.split(attrs, ';')
        for item in attrsp:
            keysp = string.split(item, '=')
            value = ''
            key = string.strip(keysp[0])
            if len(keysp) == 2:
                value = string.strip(keysp[1])
            
            keylower = string.lower(key)
            if keylower in self.default_item:
                self._default[keylower] = value
            else:
                self._cookie[key] = value
                
        
    def add_response(self, res):
        self._parse(res)
    
    def add_attr(self, key, value):
        self._cookie[key] = value
    
    def get_attr(self, key):
        try:
            ret = self._cookie[key]
        except:
            return ''
        else:
            return ret
    
    def client_string(self):
        ret = "Cookie: "
        if not self._cookie:
            return ''
        x = []
        for key in self._cookie.keys():
            if key not in self.default_item:
                x.append('%s=%s' % (key, self._cookie[key]))
        return 'Cookie: ' + '; '.join(x)
        
    def dump(self):
        buf = cStringIO.StringIO()
        cPickle.dump(self._cookie, buf)
        return buf.getvalue()
        
if __name__ == '__main__':
    simple = ClientCookie()
   
    s1 = "Set-Cookie: BDUSS=R-Mi1sMGs0eXd5RWFHcm9xMHlISnhhSEFuWWF0LU41TFJQRXN-RHgtTTRDWnBHQWdBQUFBJCQAAAAAAAAAAAAAAACmdgMBcHl0aG9uMjMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADh8ckY4fHJGRk; path=/; domain=.baidu.com";
    
    s2 = 'Set-Cookie: dotproject=063c8ecb464339e5344e551c4b4c7062; expires=Fri, 24 Jul 2009 04:49:29 GMT; path=/dotproject/'
    simple.add_response(s2)

    print simple._cookie

    print simple.client_string()
    
    #print simple.dump()



