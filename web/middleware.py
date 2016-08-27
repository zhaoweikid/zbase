#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
log = logging.getLogger()

class JSONPMiddleware:
    def before(self, viewobj, *args, **kwargs):
        return

    def after(self, viewobj, *args, **kwargs):
        input = viewobj.req.input()
        if input.get('format', '') == 'jsonp':
            if viewobj.req.method.upper() == 'GET':
                viewobj.resp.headers['Content-Type'] = 'application/javascript; charset=UTF-8'
                callback = input.get('callback','callback')
                viewobj.resp.content = '%s(%s)' % (callback, viewobj.resp.content)

        elif input.get('format', '') == 'cors':
            origin = viewobj.req.environ.get('HTTP_ORIGIN','')
            if origin:
                viewobj.resp.headers['Access-Control-Allow-Origin'] = origin
                viewobj.resp.headers['Access-Control-Allow-Credentials'] = 'true'

        return viewobj.resp

class DEBUGMiddleware:
    def before(self, viewobj, *args, **kwargs):
        return

    def after(self, viewobj, *args, **kwargs):
        #REQUEST
        #method
        log.debug('>> %s %s HTTP/1.1', viewobj.req.method, viewobj.req.path)
        #headers
        for k,v in viewobj.req.headers().iteritems():
            log.debug('>> %s:%s',k,v)
        #body
        log.debug('=> %s',viewobj.req.storage.value)

        #RESPONSE
        log.debug('<< HTTP/1.1 %s', viewobj.resp.status)
        for k,v in viewobj.resp.headers.iteritems():
            log.debug('<< %s:%s',k,v)
        log.debug('<< Content-Length:%d',len(viewobj.resp.content))
        for c in viewobj.resp.cookies.values():
            log.debug('<< Set-Cookie:%s', c.OutputString())
        log.debug('<= %s', viewobj.resp.content)

        return viewobj.resp
