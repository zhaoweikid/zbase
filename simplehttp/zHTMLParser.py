# encoding: utf-8
"""A parser for HTML and XHTML."""

# This file is based on sgmllib.py, but the API is slightly different.

# XXX There should be a way to distinguish between PCDATA (parsed
# character data -- the normal case), RCDATA (replaceable character
# data -- only char and entity references and end tags are special)
# and CDATA (character data -- only end tags are special).


import markupbase
import re
import string

# Regular expressions used for parsing

interesting_normal = re.compile('[&<]')
interesting_cdata = re.compile(r'<(/|\Z)')
interesting_script = re.compile(r'<(/script|\Z)', re.IGNORECASE)
interesting_style = re.compile(r'<(/style|\Z)', re.IGNORECASE)

incomplete = re.compile('&[a-zA-Z#]')

entityref = re.compile('&([a-zA-Z][-.a-zA-Z0-9]*)[^a-zA-Z0-9]')
charref = re.compile('&#(?:[0-9]+|[xX][0-9a-fA-F]+)[^0-9a-fA-F]')

starttagopen = re.compile('<[a-zA-Z]+[ \t]')
piclose = re.compile('>')
commentclose = re.compile(r'--\s*>')
tagfind = re.compile('[a-zA-Z][-.a-zA-Z0-9:_]*')
attrfind = re.compile(
    r'\s*([a-zA-Z_][-.:a-zA-Z_0-9]*)(\s*=\s*'
    r'(\'[^\']*\'|"[^"]*"|[-a-zA-Z0-9./,:;+*%?!&$\(\)_#=~@\\\'\"|\x81-\xff]*))?')

locatestarttagend = re.compile(r"""
  <[a-zA-Z][-.a-zA-Z0-9:_]*          # tag name
  (?:\s+                             # whitespace before attribute name
    (?:[a-zA-Z_][-.:a-zA-Z0-9_]*     # attribute name
      (?:\s*=\s*                     # value indicator
        (?:'[^']*'                   # LITA-enclosed value
          |\"[^\"]*\"                # LIT-enclosed value
          |[^'\">\s]+                # bare value
         )
       )?
     )
   )*
  \s*                                # trailing whitespace
""", re.VERBOSE)
endendtag = re.compile('>')
endtagfind = re.compile('</\s*([a-zA-Z][-.a-zA-Z0-9:_]*)\s*>')
commenttag = re.compile('\s*<!--.*|\s*-->', re.DOTALL)

class HTMLParseError(Exception):
    """Exception raised for all parse errors."""

    def __init__(self, msg, position=(None, None)):
        assert msg
        self.msg = msg
        self.lineno = position[0]
        self.offset = position[1]

    def __str__(self):
        result = self.msg
        if self.lineno is not None:
            result = result + ", at line %d" % self.lineno
        if self.offset is not None:
            result = result + ", column %d" % (self.offset + 1)
        return result


class HTMLParser(markupbase.ParserBase):
    """Find tags and other markup and call handler functions.

    Usage:
        p = HTMLParser()
        p.feed(data)
        ...
        p.close()

    Start tags are handled by calling self.handle_starttag() or
    self.handle_startendtag(); end tags by self.handle_endtag().  The
    data between tags is passed from the parser to the derived class
    by calling self.handle_data() with the data as argument (the data
    may be split up in arbitrary chunks).  Entity references are
    passed by calling self.handle_entityref() with the entity
    reference as the argument.  Numeric character references are
    passed to self.handle_charref() with the string containing the
    reference as the argument.
    """

    CDATA_CONTENT_ELEMENTS = ("script", "style")


    def __init__(self):
        """Initialize and reset this instance."""
        self.reset()

    def reset(self):
        """Reset this instance.  Loses all unprocessed data."""
        self.rawdata = ''
        self.lasttag = '???'
        self.interesting = interesting_normal
        markupbase.ParserBase.reset(self)

    def feed(self, data):
        """Feed data to the parser.

        Call this as often as you want, with as little or as much text
        as you want (may include '\n').
        """
        self.rawdata = self.rawdata + data
        self.goahead(0)

    def close(self):
        """Handle any buffered data."""
        self.goahead(1)

    def error(self, message):
        raise HTMLParseError(message, self.getpos())

    __starttag_text = None

    def get_starttag_text(self):
        """Return full source of start tag: '<...>'."""
        return self.__starttag_text

    def set_cdata_mode(self):
        self.interesting = interesting_cdata

    def clear_cdata_mode(self):
        self.interesting = interesting_normal
    
    def skip_to_char(self, i, c):
        rawdata = self.rawdata
        rlen = len(rawdata)
        for x in xrange(i, rlen):
            if rawdata[x] == c:
                return x
        return x 
    # Internal -- handle data as far as reasonable.  May leave state
    # and data to be processed by a subsequent call.  If 'end' is
    # true, force handling all data as if followed by EOF marker.
    def goahead(self, end):
        rawdata = self.rawdata
        i = 0
        n = len(rawdata)
        while i < n:
            match = self.interesting.search(rawdata, i) # < or &
            if match:
                j = match.start()
            else:
                j = n
            # 如果多个字中间有&nbsp;这样的，或造成多次调用handle_data
            if i < j: self.handle_data(rawdata[i:j])
            i = self.updatepos(i, j)
            if i == n: break
            startswith = rawdata.startswith
            #print 'start:', rawdata[i:i+10]
            if startswith('<', i):
                if starttagopen.match(rawdata, i): # < + letter
                    #print 'parse start tag'
                    k = self.parse_starttag(i)
                elif startswith("</", i):
                    #print 'parse end tag'
                    k = self.parse_endtag(i)
                elif startswith("<!--", i):
                    #print 'comment'
                    k = self.parse_comment(i)
                elif startswith("<?", i):
                    k = self.parse_pi(i)
                elif startswith("<!", i):
                    #print 'declare', i, rawdata[int(i): int(i)+6]
                    #k = self.parse_declaration(i)
                    k = self.skip_to_char(i, '>')
                elif (i + 1) < n:
                    #print 'parse data'
                    self.handle_data("<")
                    k = i + 1
                else:
                    break
                if k < 0:
                    if end:
                        self.error("EOF in middle of construct")
                    break
                i = self.updatepos(i, k)
            elif startswith("&#", i):
                match = charref.match(rawdata, i)
                if match:
                    name = match.group()[2:-1]
                    self.handle_charref(name)
                    k = match.end()
                    if not startswith(';', k-1):
                        k = k - 1
                    i = self.updatepos(i, k)
                    continue
                else:
                    break
            elif startswith('&', i):
                match = entityref.match(rawdata, i)
                if match:
                    name = match.group(1)
                    self.handle_entityref(name)
                    k = match.end()
                    if not startswith(';', k-1):
                        k = k - 1
                    i = self.updatepos(i, k)
                    continue
                match = incomplete.match(rawdata, i)
                if match:
                    # match.group() will contain at least 2 chars
                    if end and match.group() == rawdata[i:]:
                        self.error("EOF in middle of entity or char ref")
                    # incomplete
                    break
                elif (i + 1) < n:
                    # not the end of the buffer, and can't be confused
                    # with some other construct
                    self.handle_data("&")
                    i = self.updatepos(i, i + 1)
                else:
                    break
            else:
                assert 0, "interesting.search() lied"
        # end while
        if end and i < n:
            self.handle_data(rawdata[i:n])
            i = self.updatepos(i, n)
        self.rawdata = rawdata[i:]

    # Internal -- parse processing instr, return end or -1 if not terminated
    def parse_pi(self, i):
        rawdata = self.rawdata
        assert rawdata[i:i+2] == '<?', 'unexpected call to parse_pi()'
        match = piclose.search(rawdata, i+2) # >
        if not match:
            return -1
        j = match.start()
        self.handle_pi(rawdata[i+2: j])
        j = match.end()
        return j

    # Internal -- handle starttag, return end or -1 if not terminated
    def parse_starttag(self, i):
        self.__starttag_text = None
        endpos = self.check_for_whole_start_tag(i)
        #print 'parse_starttag i:', i, 'endpos:', endpos
        if endpos < 0:
            return endpos
        rawdata = self.rawdata
        self.__starttag_text = rawdata[i:endpos]
        #print 'endpos:', endpos, 'text:', self.__starttag_text
        # Now parse the data between i+1 and j into a tag and attrs
        attrs = []
        match = tagfind.match(rawdata, i+1)
        #print 'tagfind:', match
        assert match, 'unexpected call to parse_starttag()'
        k = match.end()
        self.lasttag = tag = rawdata[i+1:k].lower()
        #print 'check lasttag:', tag
        while k < endpos:
            m = attrfind.match(rawdata, k)
            if not m:
                break
            attrname, rest, attrvalue = m.group(1, 2, 3)
            #print attrname, rest, attrvalue
            if not rest:
                attrvalue = None
            elif attrvalue[:1] == '\'' == attrvalue[-1:] or \
                 attrvalue[:1] == '"' == attrvalue[-1:]:
                attrvalue = attrvalue[1:-1]
                attrvalue = self.unescape(attrvalue)
            attrs.append((attrname.lower(), attrvalue))
            k = m.end()
        # 又出错了
        if k > endpos:
            k = endpos - 1
        end = rawdata[k:endpos].strip()
        #print 'end: ', end, rawdata[k:endpos]
        if end not in (">", "/>"):
            lineno, offset = self.getpos()
            if "\n" in self.__starttag_text:
                lineno = lineno + self.__starttag_text.count("\n")
                offset = len(self.__starttag_text) \
                         - self.__starttag_text.rfind("\n")
            else:
                offset = offset + len(self.__starttag_text)
            #self.error("junk characters in start tag: %r" % (rawdata[k:endpos][:20],))
            return endpos
        if end.endswith('/>'):
            # XHTML-style empty tag: <span attr="value" />
            self.handle_startendtag(tag, attrs)
        else:
            self.handle_starttag(tag, attrs)
            #if tag in self.CDATA_CONTENT_ELEMENTS:
            #    self.set_cdata_mode()
            #print 'tag:', tag
            if tag == 'script':
                self.interesting = interesting_script
                #print 'ok, script'
            elif tag == 'style':
                self.interesting = interesting_style
        return endpos
    
    # 修正不能完全匹配一个标签里所有属性，导致出错的情况
    def revise_tag_end_flag(self, rawdata, j):
        # j是当前匹配的最后一个字符的下一个字符
        rlen = len(rawdata)
        # 计算往后继续检查的长度，往后128
        if j + 128 > rlen:
            xlen = rlen
        else:
            xlen = j + 128
        for x in xrange(j, xlen):
            #print x, rawdata[x]
            # 找到最终的>
            if rawdata[x] == '>':
                j = x
                #next = '>'
                break
        return j

    # Internal -- check to see if we have a complete starttag; return end
    # or -1 if incomplete.
    def check_for_whole_start_tag(self, i):
        rawdata = self.rawdata        
        m = locatestarttagend.match(rawdata, i)
        #print 'm:', m.group()
        if m:
            j = m.end()
            #print '1', j
            next = rawdata[j:j+1]
            #print 'last char:', rawdata[j-1] 

            '''
            # 真他妈恶心的csdn.net, 有<xxx aaa=\"19\"这样的东西
            lastchar = rawdata[j-1]
            if lastchar == '\\':
                rlen = len(rawdata)
                # 计算往后继续检查的长度，往后128
                if j + 128 > rlen:
                    xlen = rlen
                else:
                    xlen = j + 128
                for x in xrange(j, xlen):
                    #print x, rawdata[x]
                    # 找到最终的>
                    if rawdata[x] == '>':
                        j = x
                        next = '>'
                        break
            #print '2', j 
            if next == ">":
                return j + 1
            if next == "/":
                if rawdata.startswith("/>", j):
                    return j + 2
                if rawdata.startswith("/", j):
                    # buffer boundary
                    return -1
                # else bogus input
                self.updatepos(i, j + 1)
                self.error("malformed empty start tag")
            if next == "":
                # end of input
                return -1
            if next in ("abcdefghijklmnopqrstuvwxyz=/"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
                # end of input in or before attribute value, or we have the
                # '/' from a '/>' ending
                return -1
            ''' 
 
            # 肯定是格式又出问题了，那尝试猜猜
            s = m.end()
            rlen = len(rawdata)
            for x in xrange(s, rlen):
                #print 'xxxx', x, rawdata[x]
                if rawdata[x] == '>':
                    return x + 1

            self.updatepos(i, j)
            self.error("malformed start tag")
        raise AssertionError("we should not get here!")

    # Internal -- parse endtag, return end or -1 if incomplete
    def parse_endtag(self, i):
        rawdata = self.rawdata
        assert rawdata[i:i+2] == "</", "unexpected call to parse_endtag"
        match = endendtag.search(rawdata, i+1) # >
        if not match:
            return -1
        j = match.end()
        #print 'raw:', rawdata[i:i+10],'=========='
        match = endtagfind.match(rawdata, i) # </ + tag + >
        if not match:
            # 没有匹配到，可能是有</a</td>这样的情况
            #self.error("bad end tag: %r" % (rawdata[i:j],))
            start = string.find(rawdata, '</', i)
            tag = ''
            start = start + 2
            if -1 != start:
                for x in xrange(start, len(rawdata)):
                    if rawdata[x] not in string.ascii_letters:
                        break
                    tag = tag + rawdata[x]
            #print 'tag:', tag
        else:        
            tag = match.group(1)
        self.handle_endtag(tag.lower())
        self.clear_cdata_mode()
        return j

    # Overridable -- finish processing of start+end tag: <tag.../>
    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    # Overridable -- handle start tag
    def handle_starttag(self, tag, attrs):
        pass

    # Overridable -- handle end tag
    def handle_endtag(self, tag):
        pass

    # Overridable -- handle character reference
    def handle_charref(self, name):
        pass

    # Overridable -- handle entity reference
    def handle_entityref(self, name):
        pass

    # Overridable -- handle data
    def handle_data(self, data):
        pass

    # Overridable -- handle comment
    def handle_comment(self, data):
        pass

    # Overridable -- handle declaration
    def handle_decl(self, decl):
        pass

    # Overridable -- handle processing instruction
    def handle_pi(self, data):
        pass

    def unknown_decl(self, data):
        self.error("unknown declaration: %r" % (data,))

    # Internal -- helper to remove special character quoting
    def unescape(self, s):
        if '&' not in s:
            return s
        s = s.replace("&nbsp;", " ")
        s = s.replace("&lt;", "<")
        s = s.replace("&gt;", ">")
        s = s.replace("&apos;", "'")
        s = s.replace("&quot;", '"')
        s = s.replace("&amp;", "&") # Must be last
        return s
