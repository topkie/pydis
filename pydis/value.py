# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from typing import Any, Union


class Value:
    __INF = float('inf')  # 无穷大
    __slots__ = ['value', 'expire_at']

    def __init__(self, value: Any, ex: Union[int, timedelta, None]):
        self.value = value
        if ex is not None:
            if isinstance(ex, timedelta):
                ex = int(ex.total_seconds())
            self.expire_at = int(datetime.now().timestamp()) + ex
        else:
            self.expire_at = self.__INF  # 在无穷大时刻过期，即永不过期

    @property
    def expired(self) -> bool:
        return datetime.now().timestamp() > self.expire_at

    @property
    def ttl(self) -> int:
        if self.expire_at is self.__INF:
            return -1  # 表示永不过期
        return int(self.expire_at - datetime.now().timestamp())

    def cre(self, amount) -> int:
        if not isinstance(self.value, int):
            raise ValueError('type: %s not support incr/decr' % type(self.value))
        self.value += amount
        return self.value


NOT_EXISTS = Value(None, 0)
NOT_EXISTS.expire_at = float('-inf')
