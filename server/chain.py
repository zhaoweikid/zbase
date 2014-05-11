# conding: utf-8
import os, sys
import traceback
import time

class CallbackChain:
    def __init__(self, once=True):
        # callback, args
        self._callbacks = []
        self.result     = None
        self.once       = once

    def add_callback(self, succ_callback, fail_callback, succ_args, succ_kwargs, fail_args, fail_kwargs):
        self._callbacks.append([[succ_callback, succ_args, succ_kwargs], [fail_callback, fail_args, fail_kwargs]])

    def add_succback(self, callback, *args, **kwargs):
        self._callbacks.append([[callback, args, kwargs], None])


    def add_failback(self, callback, *args, **kwargs):
        self._callbacks.append([None, [callback, args, kwargs]])

    def clear(self):
        self._callbacks = []
        self.result = None

    def callback(self, succ=None, fail=None):
        self.__call__(succ, fail)

    def succ(self, ret):
        self.__call__(ret, None)

    def fail(self, ret):
        self.__call__(None, ret)

    def __call__(self, succ=None, fail=None):
        self.result = succ
        if self.once:
            while self._callbacks:
                succback, failback = self._callbacks.pop(0)
                if succ and succback:
                    callback, args, kwargs = succback
                    self.result = callback(self.result, *args, **kwargs)
                elif fail and failback:
                    callback, args, kwargs = failback
                    callback(fail, *args, **kwargs)
        else:
            for x in self._callbacks:
                succback, failback = x
                if succ and succback:
                    callback, args, kwargs = succback
                    self.result = callback(self.result, *args, **kwargs)
                elif fail and failback:
                    callback, args, kwargs = failback
                    self.result = callback(self.result, *args, **kwargs)
        return self.result 

def test():
    def callx(result):
        if result is None:
            return 1
        return result + 1

    cc = CallbackChain()
    cc.add_succback(callx) 
    cc.add_succback(callx) 
    cc.add_succback(callx) 
    cc.add_succback(callx) 

    print cc.once, cc(100)
    print cc.once, cc(100)

    cc = CallbackChain(once=False)
    cc.add_succback(callx) 
    cc.add_succback(callx) 
    cc.add_succback(callx) 
    cc.add_succback(callx) 

    print cc.once, cc(100)
    print cc.once, cc(100)

if __name__ == '__main__':
    test()


