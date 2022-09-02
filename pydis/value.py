# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from typing import Any, Union

INF = float('inf')  # 无穷大


class Value:
    __slots__ = [
        'value',
        'expire_at',
        'expiry',
    ]

    def __init__(self, value: Any, ex: Union[int, timedelta, None]):
        self.value = value
        if ex is not None:
            if isinstance(ex, timedelta):
                ex = int(ex.total_seconds())
            self.expire_at = int(datetime.now().timestamp()) + ex
            self.expiry = True

        else:
            self.expire_at = INF  # 在无穷大时刻过期，即永不过期
            self.expiry = False

    @property
    def expired(self) -> bool:
        return datetime.now().timestamp() > self.expire_at

    @property
    def ttl(self) -> int:
        if not self.expiry:
            return -1  # 表示永不过期
        return int(self.expire_at - datetime.now().timestamp())

    def cre(self, amount) -> int:
        if not isinstance(self.value, int):
            raise ValueError('type: %s not support incr/decr' % type(self.value))
        self.value += amount
        return self.value

    def __repr__(self) -> str:
        return 'Value(\'' + str(self.value) + '\', ' + str(self.expire_at) + ')'


NOT_EXISTS = Value(None, 0)
NOT_EXISTS.expire_at = float('-inf')
