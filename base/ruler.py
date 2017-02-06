#coding: utf-8

import types
import random
import operator
import json
import logging
log = logging.getLogger()

class RuleItem(object):
    '''
    代表一条规则，支持多种运算符
    name: 操作的字段
    op: 操作符
    result: 结果字段(与输入数据进行对比)
    逻辑运算：==, !=, <, <=, >, >=
    其他运算：
        startswith: 以某个字符串开头，
        endswith: 以某个字符串结尾
        in/notin: 在/不在某个列表或字符串之中
        range:在某个范围之内
        sub: 值在结果列表的某个成员中
    '''
    # 规则较多时，使用__slot__减少内存占用
    __slots__ = ("name", "op", "result")
    def __init__(self, name, op, result):
        self.name = name
        self.op = op
        self.result = result

    def __lt__(self, input):
        value = input.get(self.name, '')
        return value < self.result

    def __le__(self, input):
        value = input.get(self.name, '')
        return value <= self.result

    def __gt__(self, input):
        value = input.get(self.name, '')
        return value > self.result

    def __ge__(self, input):
        value = input.get(self.name, '')
        return value >= self.result

    def __contains__(self, input):
        value = input.get(self.name, '')
        return value in self.result

    def __ne__(self, input):
        value = input.get(self.name, '')
        return value != self.result

    def __eq__(self, input):
        value = input.get(self.name, '')
        return value == self.result

    def __getattr__(self, name):
        return self.__func_call__(name)

    def __func_call__(self, name):
        def _(input):
            return self._cmp_value_(name, input)
        return _

    def _cmp_value_(self, name, input):
        value = input.get(self.name, '')
        # 时间和数值
        if name == 'range':
            return self.result[0] <= value <= self.result[1]
        # 列表
        elif name == 'sub':
            if type(self.result) == types.ListType:
                for item in self.result:
                    if value in item:
                        return True
            return False
        elif name == 'startswith':
            if type(value) in types.StringTypes and type(self.result) in types.StringTypes:
                return value.startswith(self.result)
            return False
        elif name == 'endswith':
            if type(value) in types.StringTypes and type(self.result) in types.StringTypes:
                return value.endswith(self.result)
            return False
        elif name == 'notin':
            return not self.__contains__(input)
        else:
            log.debug('not support method')
            return False

class RuleSet(object):
    '''
    一组规则
    '''
    op_method = {
        '=':  'eq',
        '!=': 'ne',
        '>':  'gt',
        '>=': 'ge',
        '<':  'lt',
        '<=': 'le',
        'in': 'contains',
    }
    def __init__(self, rules):
        self.parse(rules)

    def parse(self, rules):
        self.rule_list = []
        for rule in rules:
            if len(rule) != 3 or type(rule) != types.ListType:
                continue
            self.rule_list.append(RuleItem(rule[0], rule[1], rule[2]))

    def match(self, input):
        match_result = 0
        # 遇到不匹配立即False
        for rule in self.rule_list:
            oper = rule.op
            if oper in RuleSet.op_method:
                if getattr(operator, RuleSet.op_method[oper])(rule, input):
                    continue
                else:
                    log.debug("rule:%s not target", str('%s %s %s' % (rule.name, rule.op, rule.result))[:100])
                    return False
                #match_result += 1 if getattr(operator, RuleSet.op_method[oper])(rule, input) else 0
            else:
                if getattr(rule, oper)(input):
                    continue
                else:
                    log.debug("rule:%s not target", str('%s %s %s' % (rule.name, rule.op, rule.result))[:100])
                    return False
                #match_result += 1 if getattr(rule, oper)(input) else 0
        return True
        #log.debug("rule list length:%d, match count:%d", len(self.rule_list), match_result)
        #return match_result == len(self.rule_list)

class RuleMatcher(object):
    '''
    规则匹配
    '''
    def __init__(self, rules = None):
        self.rules = rules
        self._fill_in(rules)

    def _fill_in(self, rules):
        self.rules['ruleset'] = RuleSet(rules.get('rule'))

    def check(self, input):
        if type(input) in types.StringTypes:
            try:
                input = json.loads(input)
            except:
                return None
        if type(input) != types.DictionaryType:
            log.info("server=Rule|error=error input type")
            return None

        if self.rules['ruleset'].match(input):
            rand_num = random.randint(1, 100)
            log.debug("rand_num: %d, rate:%s", rand_num, self.rules['rate'])
            if rand_num > int(self.rules['rate'] * 100):
                log.debug("rule not target")
                return None
            return self.rules['result'][rand_num % len(self.rules['result'])]
        return None

class RuleGroup(object):
    '''
    多组规则
    '''
    def __init__(self, grouplist = None):
        if type(grouplist) != types.ListType:
            return
        self.group = []
        for rules in grouplist:
           self.group.append(RuleMatcher(rules))

    def get_first_match(self, input):
        for matcher in self.group:
            result = matcher.check(input)
            if result:
                return result
        return None

def main():
    global log
    import logger
    log = logger.install('stdout')

    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule':[['userid', '=', '123330000000000000000000000000000000000000000000000000000000000000000000000'], ['date', 'range', ['123', '234']]]}
    match = RuleMatcher(rules)
    input = {"userid" : "12333"}
    print  match.check(input)

def test():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', 'in', ['227519', '2345']]]}
    match = RuleMatcher(rules)
    input = {"userid" : "227519"}
    print  match.check(input)

def test_ge():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', '>=', 227519]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_gt():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', '>', 200000]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_le():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', '<=', 227519]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_lt():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', '<', 227520]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_eq():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', '=', 227519]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_ne():
    rules = {"result" : ['hello', 'world'], 'rate' : 1.0, 'rule' : [['userid', '!=', 227520]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_in():
    rules = {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['userid', 'in', [227520, 227519]]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_notin():
    rules = {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['userid', 'notin', [227520, 227519]]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_range_int():
    rules = {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['userid', 'range', [227510, 227520]]]}
    match = RuleMatcher(rules)
    input = {"userid" : 227519}
    print  match.check(input)

def test_range_date():
    rules = {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['date', 'range', ['2014-09-10', '2014-09-20']]]}
    match = RuleMatcher(rules)
    input = {"date" : '2014-09-15'}
    print  match.check(input)

def test_range_time():
    rules = {"result" : ['time', 'test'], 'rate' : 1.0, 'rule' : [['time', 'range', ['00:00:00', '02:00:30']]]}
    match = RuleMatcher(rules)
    input = {"time" : '01:05:59'}
    print  match.check(input)

def test_sub():
    rules = {"result" : ['sub', 'test', 'in'], 'rate' : 1.0, 'rule' : [['name', 'sub', ['hanmeimei', 'lilei']]]}
    match = RuleMatcher(rules)
    input = {"name" : 'li'}
    print  match.check(input)

def test_startswith():
    rules = {"result" : ['start', 'with', 'word'], 'rate' : 1.0, 'rule' : [['name', 'startswith', 'hell']]}
    match = RuleMatcher(rules)
    input = {"name" : 'hello'}
    print  match.check(input)

def test_group_1():
    rules = [{"result" : ['start', 'with', 'word'], 'rate' : 1.0, 'rule' : [['name', 'startswith', 'hell']]},
                {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['date', 'range', ['2014-09-10', '2014-09-20']]]}]
    group = RuleGroup(rules)
    input = {"name" : 'hello', 'date':'2014-09-28'}
    print group.get_first_match(input)

def test_group_2():
    rules = [{"result" : ['start', 'with', 'word'], 'rate' : 1.0, 'rule' : [['name', 'startswith', 'hell']]},
                {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['date', 'range', ['2014-09-10', '2014-09-20']]]}]
    group = RuleGroup(rules)
    input = {"name" : 'xiangwei', 'date':'2014-09-18'}
    print group.get_first_match(input)

def test_group_3():
    rules = [{"result" : ['test', 'group'], 'rate' : 1.0, 'rule' : [['name', 'startswith', 'xiang'], ['userid', 'range', [227510, 227520]]]},
                {"result" : ['hello', 'world', 'test'], 'rate' : 1.0, 'rule' : [['date', 'range', ['2014-09-10', '2014-09-20']]]}]
    group = RuleGroup(rules)
    input = {"name" : 'xiangwei', 'userid' : 227519, 'date':'2014-09-28'}
    print group.get_first_match(input)

if __name__=="__main__":
    #test()
    #test_ge()
    #test_gt()
    #test_le()
    #test_lt()
    #test_eq()
    #test_ne()
    #test_in()
    #test_notin()
    #test_range_int()
    #test_range_time()
    #test_sub()
    #test_startswith()
    #test_group_1()
    #test_group_2()
    #test_group_3()
    main()
