# coding: utf-8
import string, os, sys
from zbase.simplehttp import http
from pageparser import *

def suck(url, flag=LINK_SELF, urlfilter=None, imgfilter=None):
    h = http.HTTP()
    h.debuglevel = 2 

    data = h.retr(url)
    page = PageParser()
    page.parse(url, data, flag, urlfilter, imgfilter)

    linker = page.get_linker()
    return linker, data

if __name__ == '__main__':
    linker, data = suck(sys.argv[1]) 
    print linker
    print '-'*80
    #print data
    
