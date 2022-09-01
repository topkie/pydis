# -*- coding: utf-8 -*-

from datetime import timedelta
from typing import Any, Dict, Iterable, Optional, Union

from .utils import Singleton
from .value import NOT_EXISTS, Value


class Pydis(metaclass=Singleton):

    def __init__(self, default_timeout: Optional[int] = None) -> None:
        self.default_timeout = default_timeout
        self._db: Dict[str, Value] = {}
        self.__NOT_EXISTS = NOT_EXISTS

    @property
    def empty(self) -> bool:
        return not bool(self._db)

    def get(self, key: str) -> Union[Any, None]:
        return self._get(key).value

    def _get(self, key: str) -> Value:
        try:
            value = self._db[key]
        except KeyError:
            return self.__NOT_EXISTS
        if value.expired:
            self._db.pop(key)
            return self.__NOT_EXISTS
        return value

    def set(self, key: str, value: Any,
            ex: Optional[Union[int, timedelta]] = None) -> bool:
        if value is None:
            raise ValueError('`None` is special to pydis, can not use it as a value')
        if ex is None:
            ex = self.default_timeout
        self._db[key] = Value(value, ex)
        return True

    def setnx(self, key: str, value: Any,
              ex: Optional[Union[int, timedelta]] = None) -> bool:
        if self.get(key) is not None:
            return False
        if ex is None:
            ex = self.default_timeout
        return self.set(key, value, ex)

    def delete(self, key: str) -> bool:
        try:
            self._db.pop(key)
            return True
        except KeyError:
            return False

    def _delete_many(self, keys: Iterable[str]):
        per_db = self._db
        alive_keys = set(per_db).difference(keys)
        self._db = {key: per_db[key] for key in alive_keys}

    def exists(self, key: str) -> bool:
        return self.get(key) is not None  # 过期或不存在都认为不存在

    def keys(self):
        alive_keys, expired_keys = [], []
        for key, value in self._db.items():
            if value.expired:
                expired_keys.append(key)
            else:
                alive_keys.append(key)
        if expired_keys:
            self._delete_many(expired_keys)
        return alive_keys

    def ttl(self, key: str) -> int:
        value = self._get(key)
        if value is self.__NOT_EXISTS:
            return 0
        return value.ttl

    def incr(self, key: str, amount: int = 1, ex: Optional[int] = None) -> int:
        if not isinstance(amount, int):
            raise ValueError('can not increment by type: %s' % type(amount))
        return self._cre(key, amount, ex)

    def decr(self, key: str, amount: int = 1, ex: Optional[int] = None) -> int:
        if not isinstance(amount, int):
            raise ValueError('can not decrement by type: %s' % type(amount))
        return self._cre(key, -amount, ex)

    def _cre(self, key: str, amount: int, ex: Union[int, None]) -> int:
        val = self._get(key)
        if val is self.__NOT_EXISTS:  # key 过期或不存在
            if ex is None:
                ex = self.default_timeout
            self._db[key] = Value(0, ex)
        elif ex is not None:  # key 存在，但需要重设过期时间
            self._db[key] = Value(val.value, ex)
        return self._db[key].cre(amount)

    def flushdb(self):
        self._db.clear()
