# coding: utf-8
import os, string, sys, re
from simplehttp import zHTMLParser, http, form, httputils
import urlparse, types
from zbase.base import logger
from zbase.utils.ztable import *
from zbase.base.logger import log

LINK_ALL   = 0
LINK_SELF  = 1
LINK_OTHER = 2

class Linker:
    def __init__(self, urlfilter=None, imgfilter=None):
        self.links = zTable(['url','linktext', 'title'], 
                {'url':'kn', 'linktext':'k', 'title':'k'})
        self.images = zTable(['url', 'alt'], {'url':'k', 'alt':'k'})
        if urlfilter:
            self.urlfilter = urlfilter
        else:
            self.urlfilter = lambda x:True

        if imgfilter:
            self.imgfilter = imgfilter
        else:
            self.imgfilter = lambda x:True

    def __str__(self):
        s = '====== links ======\n%s\n====== images ======\n%s\n' % (str(self.links), str(self.images))
        return s

    def clear(self):
        self.links.clear()
        self.images.clear()

    def add_link(self, url, linktext='', title=''):
        if not url and not linktext and not title:
            return
        if not url:
            url = '#'
        try:
            v = [url, linktext, title]
            if self.urlfilter(v):
                self.links.insert(v)
        except Exception, e:
            err('add_link insert error:', e)
    
    def add_link_one(self, rec):
        if not rec[0] and not rec[1] and not rec[2]:
            return
        if not rec[0]:
            rec[0] = '#'
        try:
            if self.urlfilter(rec):
                self.links.insert(rec)
        except Exception, e:
            err('add_link_one insert error:', e, rec)

    def add_image(self, url, alt=''):
        try:
            v = [url, alt]
            if self.imgfilter(v):
                self.images.insert(v)
        except Exception, e:
            err('add_image insert error:', e)
    
    def add_image_one(self, rec):
        try:
            if self.imgfilter(rec):
                self.images.insert(rec)
        except Exception, e:
            err('add_image_one insert error:', e)


# 解析html, 分析出里面的链接,图片和表单, 待分析的文本应该是unicode的
class PageParser(zHTMLParser.HTMLParser):
    def __init__(self):
        zHTMLParser.HTMLParser.__init__(self)
        self.url = ""
        self.domain = ""
        self.charset = 'gbk'
        self.form = form.HTMLForm()
        self.have_form = False
        
        self.linker = None

        # 上一次开始出现a标签
        self._a_start = False
        # 上次a标签的链接
        self._a_linker = ['', '', '']
        # 当前form标示
        self._form_name = ''

        # 是否只收录本站点内部的, 0为都收录, 1为只收录本站的, 2为只收录其他站点的
        self.itself = 0
        
    def parse(self, url, data, only=0, urlfilter=None, imgfilter=None):
        '''url: 网址 data: 要解析的html数据 only: 链接获取模式 0为所有链接，1为只本站，2为不包括本站'''
        if type(data) != types.UnicodeType:
            raise ValueError, "data must be unicode string"
        self.url = url
        self.domain = httputils.domain_get(url)
        self.itself = only
        self.linker = Linker(urlfilter, imgfilter)
        self.feed(data)

    def get_linker(self):        
        return self.linker

    def get_form(self):
        return self.form
    
    def make_full_link(self, url):
        '''
        创建绝对路径
        '''
        if not url:
            return ''
        url = string.strip(url)
        if url[0] == '#':
            return ''
        # url里是不应该有+的
        a = string.find(url, '+') 
        if a != -1:
            return ''
        if url[0:7] == "mailto:":
            return url 
        if url[0:4] == "ftp:":
            return url 
        if url[0:11] == "javascript:":
            return url
        # 去掉本页面中的瞄点
        a = string.find(url, "#") 
        if a != -1:
            url = url[0:a]

        if string.find(url, "http://") != -1 or string.find(url, "https://") != -1:
            return url

        us = urlparse.urlparse(self.url)
        rooturl = "http://" + us[1]
        
        # 判断是目录还是文件, 计算出当前目录baseurl
        if us[2]:
            ext = os.path.splitext(us[2])
            if ext[1]: # 多级目录
                dir = os.path.dirname(ext[0])
            else: # 文件或者单级目录
                dir = ext[0]
            baseurl = "http://" + us[1] + dir
        else:
            baseurl = "http://" + us[1]
           
        #去掉末尾的/，为什么呢
        if baseurl[-1] == '/':
            baseurl = baseurl[:-1]
        if rooturl[-1] == '/':
            rooturl = rooturl[:-1]
        
        # 第一字母为/，表示绝对路径
        if url[0] == '/':
            #url = unicode(rooturl) + unicode(url, self.charset)
            url = rooturl + url
        elif url[0] == '?': # 直接是参数
            url = rooturl + us[2] + url
        elif url.startswith('./'): # 相对当前路径，去掉
            url = baseurl + url[1:]    
        else: # 相对路径，前面需要加上当前路径
            #url = unicode(baseurl) + u'/' + unicode(url, self.charset)
            url = baseurl + '/' + url
        return url         

    def clear_prev_url(self, url):
        '''
        去除URL中的../
        '''
        if not url:
            return ''
        urlsp = urlparse.urlparse(url)
        urlsp = list(urlsp)
        if urlsp[2]:
            pathsp = string.split(urlsp[2], '/')
            count = []
            for x in xrange(0, len(pathsp)):
                if pathsp[x] == '..':
                    count.append(x) 
            havedeal= 0
            for x in count:
                x = x - havedeal
                if x >= 1:
                    del pathsp[x]
                    del pathsp[x-1]
                    havedeal = havedeal + 2
            urlsp[2] = string.join(pathsp, '/')
        newurl = urlparse.urlunparse(urlsp)
        return newurl
    
    def _url_check (self, url):
        '''
        检查url是否合法
        '''
        if url[0:11] == 'javascript:':
            return True
        if len(url) <= 5:
            return False
        if string.find(url, '.') == -1:
            return False
        if string.find(url, '*') >= 0:
            return False
        return True

    def handle_starttag(self, tag, attrs):
        #print 'tagname:', tag
        if tag == 'a':
            self._a_linker = ['','','']
            self._a_start = True
            titlevalue = ''
            for name,value in attrs:
                #print 'attr:', name, value
                if name == 'href':
                    link = string.strip(value, "'\" ")
                    #print 'href:',link
                    link = self.make_full_link(link)
                    #print 'make_full_link', link
                    link = self.clear_prev_url(link)
                    #print 'clear_prev_url', link
                    if link:
                        if self.itself > 0: # 收录本站或者他站
                            #print 'get domain:', link
                            try:
                                domain = httputils.domain_get(link)
                            except Exception, e:
                                err("domain_get error:", e)
                            else:
                                if self.itself == 1: # 只收录本站
                                    # 这里认为javascript的都是本站的
                                    if link[0:11] != 'javascript:':
                                        if domain != self.domain:
                                            break
                                elif self.itself == 2:
                                    # 不收录本站
                                    if domain == self.domain:
                                        break
                        
                        #print 'link:', link
                        if link[0:2] == '\\"' or link[0:2] == "\\'":
                            link = link[2:-2]
                        # 这里认为网址字符串长度必须是大于5的
                        if self._url_check(link):
                            #print 'add:', link
                            self._a_linker[0] = link
                            self._a_start = True
                            #print 'add ok:', self._a_link
                elif name == 'title':
                    titlevalue = string.strip(value)
            # 有可能a的属性里,title出现在href的前面
            if titlevalue:
                #print 'add title:', self._a_link, titlevalue
                self._a_linker[2] = titlevalue

        elif tag == "meta":
            x1 = ''
            x2 = ''
            for name, value in attrs:
                #print name, value
                if name == "http-equiv":
                    x1 = string.lower(string.strip(value))
                elif name == 'content':
                    x2 = string.lower(value)
            if x1 and x1 == "content-type":
                content = string.split(x2, ';')
                if len(content) == 2:
                    charset = string.split(content[1], '=')
                    if len(charset) == 2:
                        self.charset = string.strip(charset[1]).lower()
        elif tag == 'rss': # 不分析rss的东西
            self.error("can not analyse rss.")
        elif tag == 'form':
            action = ''
            fname = ''
            method = 'GET'
            for name, value in attrs:
                if name == 'action':
                    action = value
                elif name == 'name':
                    fname = value
                elif name == 'method':
                    method = value
            if fname:
                # 对于form的名字重复的，暂时不要第二次以后的
                try:
                    self._form_name = self.form.add_form(fname, action, method)
                except Exception, e:
                    err('add_from: %s' % str(e))
                else:
                    self.have_form = True
        elif tag == 'img':
            isrc = ''
            ialt = ''
            for name, value in attrs:
                if name == 'src':
                    isrc = self.make_full_link(value)
                    isrc = self.clear_prev_url(isrc)

                elif name == 'alt':
                    ialt = value
                    if self._a_start:
                        # 如果是图片链接，图片的说明文字要记录到链接文字里吗？
                        #self.linker.add_url_text(self._a_link, value)
                        self._a_linker[1] = value
            self.linker.add_image(isrc, ialt)

        elif tag == 'frame' or tag == 'iframe':
            for name, value in attrs:
                if name == 'src':
                    if self._url_check(value):
                        self.linker.add_link(value)
        elif tag == 'td' or tag == 'tr' or tag == 'table' or tag == 'script' or tag == 'style':
            # 有的时候<a标签居然没有对应的</a
            if self._a_start:
                self.linker.add_link_one(self._a_linker)
                self._a_start = False
                self._a_linker = ['','','']

        if self.have_form:
            if tag == 'input' or tag == 'button' or tag == 'textarea' or tag == 'option':
                fname = ''
                fvalue = ''
                for name, value in attrs:
                    #print tag, name, value
                    if name == 'name':
                        fname = value
                    elif name == 'value':
                        fvalue = value
                #print '============', fname, fvalue

                self.form.add_tag(self._form_name, fname, fvalue, tag)
    
    def handle_endtag(self, tag):
        if tag == 'form':
            self.have_form = False
            self._form_name = ''
        elif tag == 'a':
            self.linker.add_link_one(self._a_linker)
            self._a_start = False
            self._a_linker = ['','','']

    def handle_data(self, data):
        '''处理链接文字'''
        if self._a_start:
            self._a_linker[1] += string.strip(data)
            #self.linker.add_link_one(self._a_linker)

def parse_data(url, data, flag=0):
    page = PageParser()
    page.parse(url, data, flag)

    linker = page.get_linker()
    return linker


def parse_page(url, region='', flag=0):
    h = simplehttp.http.HTTP()
    h.debuglevel = 0

    data = h.retr(url) 
    
    if region:
        ret = re.search(region, data)
        if not ret:
            return None
        data = ret.groups()[0]
    return parse_data(url, data, flag)


if __name__ == '__main__':
    import simplehttp
        
    url = sys.argv[1]
    
    #filelog_init()
    logger.install()
    linker = parse_page(url)

    print linker


