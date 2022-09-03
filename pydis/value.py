# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Union

INF = float('inf')  # 无穷大


class Value:
    '''值的包装类

    Attributes:
        value (Any): 存入的原始值
        expire_at (timedelta): 失效时刻，以 datetime 保存
        expiry (bool): 是否会失效的标志
    '''
    __slots__ = [
        'value',
        'expire_at',
        'expiry',
    ]

    def __init__(self, value: Any, ex: Union[float, timedelta, None]):
        self.value = value
        if ex is not None:
            if isinstance(ex, (int, float)):
                ex = timedelta(seconds=ex)
            self.expire_at = datetime.now() + ex
            self.expiry = True

        else:
            self.expire_at = datetime.max
            self.expiry = False

    @property
    def expired(self) -> bool:
        '''是否失效'''
        return datetime.now() >= self.expire_at

    @property
    def ttl(self) -> int:
        if not self.expiry:
            return -1  # 表示永不过期
        return int((self.expire_at - datetime.now()).total_seconds())

    def cre(self, amount) -> int:
        if not isinstance(self.value, int):
            raise ValueError('type: %s not support incr/decr' % type(self.value))
        self.value += amount
        return self.value

    def __repr__(self) -> str:
        return 'Value(\'' + str(self.value) + '\', ' + str(self.expire_at) + ')'


NOT_EXISTS = Value(None, 0)
NOT_EXISTS.expire_at = datetime.min
