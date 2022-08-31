# -*- coding: utf-8 -*-

from datetime import timedelta
from typing import Any, Dict, Iterable, Optional, Union

from .utils import Singleton
from .value import Value


class Pydis(metaclass=Singleton):

    def __init__(self, default_timeout: Optional[int] = None) -> None:
        self.default_timeout = default_timeout
        self._db: Dict[str, Value] = {}

    def get(self, key: str) -> Union[Any, None]:
        try:
            value = self._db[key]
        except KeyError:
            return None
        if value.expired:
            self._db.pop(key)
            return None
        return value.value

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
        if self.get(key) is None:
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

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def keys(self):
        alive_keys = []
        for key, value in self._db.items():
            if value.expired:
                self.delete(key)
            else:
                alive_keys.append(key)
        return alive_keys

    def ttl(self, key: str) -> int:
        value = self.get(key)
        if value is None:
            return 0
        return self._db[key].ttl

    def flushdb(self):
        self._db.clear()
