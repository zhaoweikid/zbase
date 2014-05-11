# coding: utf-8
import string, os, sys, types
import log

class Words (object):
    def __init__(self, wordfile, enc='utf-8'):
        self.words = set()
        
        f = open(wordfile, 'r')
        maxwlen = 0
        while True:
            line = f.readline()
            if not line: break
            line = line.strip()
            if not line:
                continue
            line = unicode(line, enc)
            wlen = len(line)
            if wlen > maxwlen:
                maxwlen = wlen
            self.words.add(line)
        f.close() 
        
        self.word_max_len = maxwlen
        log.info('word max len:', maxwlen)
    
        self._init()

    def _init(self):
        self.symbol_string = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\t\r\n '
        self.symbol  = tuple([x for x in self.symbol_string])
        self.letter_string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-'
        self.letters = tuple([x for x in string.ascii_letters])
        self.number_string = '0123456789.'
        self.number  = tuple([x for x in '0123456789'])
    
    def sentence(self, text):
        return [text]
        
    def split(self, text):
        secs = self.sentence(text)
        dict_words = []
        other_words = []

        for sec in secs:
            check_end = len(sec)
            while check_end > 0:
                #loginfo('check_end:', check_end )
                # 最后的字符是一些特殊符号
                lastc = sec[check_end-1]
                if lastc in self.symbol_string:
                    #loginfo('end with symbol: ', lastc)
                    slen = 0
                    for k in xrange(check_end-1, -1, -1):
                        if sec[k] in self.symbol_string:
                            slen += 1
                        else:
                            break
                    check_end -= slen
                    #print 'skip:', slen, sec[check_end]
                    continue
                # 最后字符是英文字母，可能是单词
                elif lastc in string.ascii_letters:
                    #loginfo('end with letter:', lastc)
                    enlen = 0
                    for k in xrange(check_end-1, -1, -1):
                        if sec[k] in self.letter_string:
                            enlen += 1
                        else:
                            k += 1
                            break
                    if enlen > 1: # 两个字母以上才算单词
                        w = sec[k:k+enlen].lower()
                        
                        if w in self.words:
                            dict_words.append(w)
                            if type(self.words) == types.DictType:
                                v = self.words[w]
                                if v >= 0:
                                    self.cate_count[v] += 1
                                    self.cate_words[v].append(w)
                        else:
                            other_words.append(w)
                        #loginfo( 'found english word:', w )
                    #else:
                        #print 'only one:', sec[k:k+enlen]
                    check_end -= enlen
                    continue 
                # 最后字符是数字
                elif lastc in string.digits:
                    #loginfo('end with number:', lastc)
                    nlen = 0
                    for k in xrange(check_end-1, -1, -1):
                        if sec[k] in self.number_string:
                            nlen += 1
                        else:
                            k += 1
                            break
                    n = sec[k:k+nlen]
                    other_words.append(n)
                    #loginfo('found number word:', n)
                    check_end -= nlen
                    continue
                    

                # 检查的时候至多只在最后检测词库中词的最大长度
                if check_end > self.word_max_len:
                    check_start = check_end - self.word_max_len
                else:
                    check_start = 0


                while check_end >= 0 and check_start < check_end:
                    key = sec[check_start:check_end]
                    # 希望先把最后的的英文单词识别出来
                    #loginfo('key:|||'+key+'|||')
                                
                    #print 'check:', key, check_start, check_end
                    if key in self.words:
                        dict_words.append(key)
                        if type(self.words) == types.DictType:
                            v = self.words[key]
                            if v >= 0:
                                self.cate_count[v] += 1
                                self.cate_words[v].append(key)
 
                        #loginfo('found word:', key)
                        check_end -= len(key)
                        break
                    else:
                        check_start += 1
                else:
                    check_end -= 1
        return dict_words, other_words
        

class CateWords(Words):
    def __init__(self, dictfile, catefile, catemap, enc='utf-8'):
        self.words = {}
        self.catemap = catemap

        f = open(dictfile, 'r')
        maxwlen = 0
        while True:
            line = f.readline()
            if not line: break
            line = line.strip()
            if not line:
                continue
            line = unicode(line, enc)
            wlen = len(line)
            if wlen > maxwlen:
                maxwlen = wlen
            self.words[line] = -1
        f.close() 
        
        self.word_max_len = maxwlen
        log.info('word max len:', maxwlen)
        
        self._load_category_word(catefile, enc)
        self._init()

        
    def _load_category_word(self, filename, enc):
        self.cate_count = [0]
        self.cate_words = [[]]
        self.cates = ['base']
        f = open(filename)
        
        # 标示当前是哪个分类，默认是第一个基本分类
        cid = 0
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            line = unicode(line, enc)
            if line.startswith('--'):
                catename = line[2:]
                self.cates.append(catename)
                self.cate_count.append(0)
                self.cate_words.append([])

                cid += 1
            else:
                self.words[line] = cid
    
                
        f.close()


    def split(self, text):
        for i in xrange(0, len(self.cate_count)):
            self.cate_count[i] = 0
            self.cate_words[i] = []

        return super(CateWords, self).split(text) 

    def check_cate(self, basen=2, maxn=2):
        # 调试信息
        for i in xrange(0, len(self.cates)):
            log.info('cate: ', self.cates[i], '\tcount:', self.cate_count[i])
            log.info('words:', *self.cate_words[i])

        # base分类的词必须在basen数量以上
        if self.cate_count[0] < basen:
            return -1
        # python分类的词必须在basen数量以上
        if self.cate_count[1] < basen:
            return -2
        m = 0
        maxi = 0
        # 计算次数最多的分类id和次数
        for i in xrange(2, len(self.cate_count)):
            v = self.cate_count[i]
            if v > m:
                m = v
                maxi = i
        # 词数最多的分类的词数必须在maxn以上
        if m >= maxn:
            return maxi
                
        return -3

    def check_cateid(self, basen=2, maxn=2):
        cid = self.check_cate(basen, maxn)
        if cid > 0 and self.catemap:
            log.info('cate:', self.cates[cid])
            if self.catemap.has_key(self.cates[cid]):
                return self.catemap[self.cates[cid]]
            return 0
        else:
            return cid

def test_word():        
    import time
    filename = "/home/zhaowei/word.lib"
    x = Words(filename, 'gbk')
    
    f = open(sys.argv[1], 'r')
    a = f.read()
    f.close()
    a = unicode(a, 'gbk')
    log.info('load word ok, try split ...')
    startx = time.time()
    ret1, ret2 = x.split(a)
    endx = time.time()
    log.info('use time:', endx - startx)
    #for b in ret:
    #    print b
    log.info('words:', len(ret1), len(ret2))
    log.info('speed:', len(a) / (endx-startx))
    
    f = open('result.txt', 'w')
    for b in ret1:
        b += '\n'
        f.write(b.encode('utf-8'))
    f.close() 
   
    f = open('result2.txt', 'w')
    for b in ret2:
        b += '\n'
        f.write(b.encode('utf-8'))
    f.close() 
   
def test_cateword():
    import time
    filename = "/home/zhaowei/word.lib"
    catename = "/home/zhaowei/python.lib"
    x = CateWords(filename, catename, 'gbk')
    
    f = open(sys.argv[1], 'r')
    a = f.read()
    f.close()
    a = unicode(a, 'gbk')
    log.info('load word ok, try split ...')
    startx = time.time()
    ret1, ret2 = x.split(a)
    endx = time.time()
    log.info('use time:', endx - startx)
    #for b in ret:
    #    print b
    log.info('words:', len(ret1), len(ret2))
    log.info('speed:', len(a) / (endx-startx))
    
    for i in xrange(0, len(x.cates)):
        log.info(x.cates[i], x.cate_count[i])
    
    xi = x.check_cate()
    log.info('result:', xi, x.cates[xi])

    f = open('result.txt', 'w')
    for b in ret1:
        b += '\n'
        f.write(b.encode('utf-8'))
    f.close() 
   
    f = open('result2.txt', 'w')
    for b in ret2:
        b += '\n'
        f.write(b.encode('utf-8'))
    f.close() 
 
if __name__ == '__main__':
    #test_cateword()
    test_word()


