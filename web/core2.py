# coding: utf-8
import os, sys
import re, time, types
from zbase.web import template, reloader
from zbase.base import dbpool
from zbase.web.http import Request, Response, NotFound
from zbase.web.validator import Validator, ValidatorError
import traceback, logging
from http import MethodNotAllowed, HTTP_STATUS_CODES

log = logging.getLogger()

class HandlerFinish(Exception):
    pass

class Handler(object):
    def __init__(self, app, req):
        self.webapp = app
        self.req = req
        #self.ses = session.Session(app.settings.SESSION, req.cookie)
        self.ses = {}
        self.resp = Response()
        self.write = self.resp.write
        self.method_decorators = []

    def initial(self):
        pass

    def finish(self):
        #self.ses.end()
        pass

    def get_cookie(self, cookie_name):
        return self.req.cookie.get(cookie_name, '')

    def set_cookie(self, *args, **kwargs):
        self.resp.set_cookie(*args, **kwargs)

    def set_headers(self, headers={}):
        if headers:
            self.resp.headers.update(headers)

    def redirect(self, *args, **kwargs):
        return self.resp.redirect(*args, **kwargs)

    def GET(self):
        self.resp = MethodNotAllowed()

    POST = HEAD = DELETE = GET

    def render(self, *args, **kwargs):
        if template.render:
            kwargs.update({
                '_handler':self
            })
            self.write(template.render(*args, **kwargs))


class RouteHandler (Handler):
    def GET(self, *args, **kwargs):
        if kwargs:
            funcname = kwargs.get('func')
            func = getattr(self, funcname)
            return func(*args, **kwargs)
        self.resp = http.NotFound('func not found')



    





mimetypes = {'.js':'application/x-javascript',
            '.css':'text/css',
            '.html':'text/html',
            '.gif':'image/gif',
            '.jpg':'image/jpg',
            '.jpeg':'image/jpg',
            '.png':'image/png',
            '.svg':'image/svg+xml',
            }



class WebApplication(object):
    def __init__(self, settings):
        '''
        settings:
            DOCUMENT_ROOT: web root path
            DEBUG: True/False
            CHARSET: utf-8
            LOGGER: log file
            HOME: project home path
            TEMPLATE: {'path':xx,'tmp':xx,'cache':True}
            DATABASE: database config
            APPS: app
            URLS: (('/', index.Index), )
            STATICS
            SESSION
            MIDDLEWARE
        '''
        # 切换到字典static,兼容列表型
        if isinstance(settings.STATICS, list) or isinstance(settings.STATICS, tuple):
            settings.STATICS = dict(zip(settings.STATICS,settings.STATICS))
        
        self.allowed_methods = set(('GET', 'HEAD', 'POST', 'DELETE', 'PUT'))
        self.charset = 'utf-8'

        self.urls = []
        self.settings = settings
        self.install()

        self.add_urls(self.settings.URLS)

        if not self.settings.DOCUMENT_ROOT:
            self.document_root = os.getcwd()
        else:
            self.document_root = self.settings.DOCUMENT_ROOT

        self.debug = settings.DEBUG
        self.charset = settings.CHARSET

        self.reloader = None
        if self.debug:
            self.reloader = reloader.Reloader()


    def add_urls(self, urls, appname=''):
        # url: path, object, dict param, fields define
        tmpurls = []
        if appname:
            appname = '/' + appname

        for item in urls:
            if type(item[1]) == types.StringType: # object is a string, import
                parts = item[1].split('.')
                obj   = __import__(parts[0])
                for p in parts[1:]:
                    obj = getattr(obj, p)
            else:
                obj = item[1]

            if appname:
                pathre = re.compile(appname + item[0])
            else:
                pathre = re.compile(item[0])

            dictparam = {}
            if len(item) >= 3:
                dictparam = item[2]

            fieldsdef = []
            if len(item) == 4:
                fieldsdef = item[3]

            tmpurls.append((pathre, obj, dictparam, fieldsdef))

        #self.urls = tmpurls + self.urls
        self.urls += tmpurls

    def install(self):
        if self.settings.HOME not in sys.path:
            sys.path.insert(0, self.settings.HOME)

        tplcf = self.settings.TEMPLATE
        if tplcf['tmp'] and not os.path.isdir(tplcf['tmp']):
            os.mkdir(tplcf['tmp'])
        if tplcf['path']:
            template.install(tplcf['path'], tplcf['tmp'], tplcf['cache'],
                             self.settings.CHARSET)

        if self.settings.DATABASE:
            dbpool.install(self.settings.DATABASE)

        for appname in self.settings.APPS:
            self.add_app(appname)

    def run(self, host='0.0.0.0', port=8000):
        from gevent.wsgi import WSGIServer

        server = WSGIServer((host, port), self)
        server.backlog = 512
        try:
            log.info("Server running on %s:%d" % (host, port))
            server.serve_forever()
        except KeyboardInterrupt:
            server.stop()


    def add_app(self, appname):
        log.debug('add app:%s', appname)
        m = __import__(appname)
        self.add_urls(m.URLS, appname)

    def __call__(self, environ, start_response):
        times = [time.time()]
        req  = None
        resp = None
        viewobj = None
        try:
            if self.reloader:
                self.reloader()
            req = Request(environ)
            times.append(time.time())
            if req.path.startswith(tuple(self.settings.STATICS.keys())):
                fpath = self.document_root +  req.path
                resp = NotFound('Not Found: ' + fpath)
                for k,v in self.settings.STATICS.iteritems():
                    if req.path.startswith(k):
                        fpath = fpath.replace(k,v)
                        if os.path.isfile(fpath):
                            resp = self.static_file(req, fpath)
            else:
                for regex, view, kwargs, fields in self.urls:
                    match = regex.match(req.path)
                    if match is not None:
                        if req.method not in self.allowed_methods:
                            raise NotImplemented()
                        args    = ()
                        mkwargs = match.groupdict()
                        if mkwargs:
                            kwargs.update(mkwargs)
                        else:
                            args = match.groups()

                        times.append(time.time())

                        viewobj = view(self, req)

                        middleware = []
                        try:
                            vali = Validator(fields)
                            ret = vali.verify(req.input())
                            if ret:
                                log.debug('input verify error:%s', str(ret))
                                resp = Response(HTTP_STATUS_CODES[400] + ' ' + ','.join(ret), 400)
                                raise ValidatorError
                            viewobj.input = vali.data
                            viewobj.initial()

                            for x in self.settings.MIDDLEWARE:
                                obj = x()
                                resp = obj.before(viewobj, *args, **kwargs)
                                if resp:
                                    log.debug('middleware return:%s', resp)
                                    break
                                middleware.append(obj)
                                
                            func = getattr(viewobj, ret.method)
                            for f in viewobj.method_decorators:
                                func = f(func)
                            ret = func(*args, **kwargs)
                            viewobj.finish(ret)
                        
                            resp = viewobj.resp
                            for obj in middleware:
                                resp = obj.after(viewobj)
     
                            #if ret and (isinstance(ret, str) or isinstance(ret, unicode)):
                            #    viewobj.resp.write(ret)
                        except HandlerFinish:
                            log.debug('raise finish')

                        break
                else:
                    resp = NotFound('Not Found')
        except ValidatorError, e:
            times.append(time.time())
            log.debug(e)
        except Exception, e:
            times.append(time.time())
            log.debug(e)
            log.warn('web call error: %s', traceback.format_exc())
            if self.debug:
                resp = Response('<pre>%s</pre>' % traceback.format_exc(), 500)
            else:
                resp = Response('some error', 500)


        times.append(time.time())
        #s = '%s %s %s ' % (req.method, req.path, str(viewobj.__class__)[8:-2])
        s = [str(resp.status), req.method, req.path]
        s.append('%d' % ((times[-1]-times[0])*1000000))
        s.append('%d' % ((times[1]-times[0])*1000000))
        s.append('%d' % ((times[-1]-times[-2])*1000000))
        try:
            if req.query_string:
                s.append(req.query_string[:1024])
            if req.method == 'POST':
                s.append(str(req.input())[:1024])
            if type(req.storage.value) == str:
                s.append(req.storage.value)
            if resp.content and resp.headers['Content-Type'].startswith('application/json'):
                s.append(str(resp.content)[:1024])
        except:
            log.warn(traceback.format_exc())
        if not req.path.startswith(tuple(self.settings.STATICS.keys())):
            log.info('|'.join(s))

        return resp(environ, start_response)

    def static_file(self, req, fpath):
        global mimetypes
        extname = os.path.splitext(req.path)[1].lower()
        mtype = mimetypes.get(extname, 'application/octet-stream')
        try:
            reqgmt = req.headers['If-Modified-Since']
            reqgmt = reqgmt[:reqgmt.find('GMT') + 3]
            reqtm  = time.strptime(reqgmt, '%a, %d %b %Y %H:%M:%S GMT')
            if type(reqtm) != types.FloatType:
                reqtm = time.mktime(reqtm) + (time.mktime(time.localtime()) - time.mktime(time.gmtime()))
        except:
            reqtm  = 0
        #log.info("static:%s", fpath)
        mtime = os.path.getmtime(fpath)
        gmt   = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(mtime))
        #log.info('file time:', mtime, gmt, 'req time:', reqtm, reqgmt, int(mtime) > int(reqtm), mtype)
        if mtime > reqtm or mtype == 'application/octet-stream':
            with open(fpath, 'rb') as f:
                s = f.read()
            resp = Response(s, mimetype=mtype)
        else:
            resp = Response('', status=304, mimetype=mtype)
        resp.headers['Last-Modified'] = gmt
        return resp




