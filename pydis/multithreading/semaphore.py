# -*- coding: utf-8 -*-

from threading import Condition, Lock
from time import monotonic as _time


class Semaphore:
    '''与 threading.Semaphore 方向相反的信号量对象，用于计数

    内部管理一个计数器，``incr`` 使计数器递增，``decr``使计数
    器递减，当计数器的值小于或等于 0 时，``wait`` 方法将被阻塞，
    直到计数器的值大于 0
    '''

    def __init__(self) -> None:
        self._cond = Condition(Lock())
        self._value = 0

    def incr(self, amount: int = 1):
        with self._cond:
            self._value += amount
            self._cond.notify()

    def decr(self, amount: int = 1):
        with self._cond:
            self._value -= amount

    def wait(self, blocking=True, timeout=None):
        '''等待直到计数器的值大于 0 或超时

        返回在等待时间内计数器的值是否大于 0
        '''
        rc = False
        endtime = None
        with self._cond:
            while self._value <= 0:
                if not blocking:
                    break
                if timeout is not None:
                    if endtime is None:
                        endtime = _time() + timeout
                    else:
                        timeout = endtime - _time()
                        if timeout <= 0:
                            break
                self._cond.wait(timeout)
            else:
                rc = True
        return rc
