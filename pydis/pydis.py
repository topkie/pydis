# -*- coding: utf-8 -*-

from datetime import timedelta
from typing import Any, Collection, Dict, List, Optional, Union

from .utils import Singleton
from .value import NOT_EXISTS, Value


class Pydis(metaclass=Singleton):
    '''基于 dict 的内存管理工具

    可通过改变 ``default_timeout`` 属性改变全局失效时间，
    新的失效时间只对新存入的键有效，改变前存入的键不受影响

    采用单例模式，因此在一个进程中只存在唯一的 Pydis 对象。
    尽管如此，使用初始化传参的方法仍然能够改变 ``default_timeout``, eg:
        >>> p = Pydis()
        >>> p.default_timeout
        None
        >>> p = Pydis(5)
        >>> p.default_timeout
        5
    也可以对 ``default_timeout`` 赋值直接改变，eg:
        >>> p = Pydis()
        >>> p.default_timeout
        None
        >>> p.default_timeout = 5
        >>> p.default_timeout
        5

    Attributes:
        default_timeout (int): 全局的失效时间，默认为 None， 表示永远有效
    '''

    def __init__(self, default_timeout: Optional[int] = None) -> None:
        '''初始化 Pydis 对象

        Args:
            default_timeout (int, optional): 全局的失效时间，默认为 None， 表示永远有效
        '''
        self._setconfig(default_timeout)
        self._db: Dict[str, Value] = {}

    def _setconfig(self, default_timeout: Union[int, None]):
        self.default_timeout = default_timeout

    @property
    def empty(self) -> bool:
        return not bool(self._db)

    def get(self, key: str) -> Union[Any, None]:
        '''获取指定 key 的值

        当传入的 key 不存在或失效时返回 None.

        Args:
            key (str): 用于取值的 key.

        Returns:
            Union[Any, None]: key 对于的值，不存在或失效为 None
        '''
        return self._get(key).value

    def _get(self, key: str) -> Value:
        try:
            value = self._db[key]
        except KeyError:
            return NOT_EXISTS
        if value.expired:
            self._db.pop(key)
            return NOT_EXISTS
        return value

    def set(self, key: str, value: Any,
            ex: Optional[Union[int, timedelta]] = None) -> bool:
        '''将 ``key`` 的值设为 ``value``，``value`` 不能为 None

        ex 用于指定失效时间，可接受 int 和 timedelta 类型，
        默认为 None 表示永远有效

        本操作不会失败，因此返回值恒为 True

        Args:
            key (str): 指定的 key
            value (Any): 待设定的值
            ex (Union[int, timedelta], optional): 失效时间. 默认为 None

        Raises:
            ValueError: 传入的 ``value`` 为 None 时引发

        Returns:
            bool: True
        '''
        if value is None:
            raise ValueError('`None` is special to pydis, can not use it as a value')
        if ex is None:
            ex = self.default_timeout
        self._db[key] = Value(value, ex)
        return True

    def setnx(self, key: str, value: Any,
              ex: Optional[Union[int, timedelta]] = None) -> bool:
        '''当 ``key`` 不存在时，将 ``key`` 的值设为 ``value``

        Args:
            key (str): 待设定的键
            value (Any): 待设定的值
            ex (Union[int, timedelta], optional): 失效时间. 默认为 None

        Returns:
            bool: 操作是否成功
        '''
        if self.get(key) is not None:
            return False
        if ex is None:
            ex = self.default_timeout
        return self.set(key, value, ex)

    def mget(self, keys: Collection[str]) -> List[Any]:
        '''获取通过 ``keys`` 指定的键的值

        返回一个列表，长度与 ``keys`` 的长度相等，值的位置
        与 ``keys`` 中键的位置一一对应，不存在或失效的键其值用 None 填充

        Args:
            keys (Collection[str]): 键的集合

        Returns:
            List[Any]: 与 ``keys`` 中的键对应的值，不存在的用 None 填充
        '''
        values, expired_keys = [], []
        for key in keys:
            try:
                val = self._db[key]
                if val.expired:
                    values.append(None)
                    expired_keys.append(key)
                else:
                    values.append(val.value)
            except KeyError:
                values.append(None)
                continue
        self._delete_many(expired_keys)
        return values

    def mset(self, data: Dict[str, Any],
             ex: Optional[Union[int, timedelta]] = None) -> bool:
        '''存入通过 ``data`` 指定的键值对。

        ``data`` 需为 dict 类型。

        ``ex`` 用于指定失效时间，可接受 int 和 timedelta 类型

        本操作不会失败，因此返回值恒为 True

        Args:
            data (Dict[str, Any]): 待存入的键值对
            ex (Union[int, timedelta], optional): 失效时间. 默认为 None

        Returns:
            bool: True
        '''
        if ex is None:
            ex = self.default_timeout
        self._db.update({key: Value(val, ex) for key, val in data.items()})
        return True

    def msetnx(self, data: Dict[str, Any],
               ex: Optional[Union[int, timedelta]] = None) -> int:
        '''存入通过 dict 指定的多个键值对

        与 redis 不同的是，如果传入本方法的部分 key 已经存在，
        这些 key 将会被忽略， pydis 会将剩下键值对存起来。

        Args:
            data (Dict[str, Any]): 待存储的键值对
            ex (int | timedelta, optional): 失效时间. 默认为 None.

        Returns:
            int: 成功存储的键值对的数量
        '''
        if ex is None:
            ex = self.default_timeout
        set_keys = set(data).difference(self._db)
        self._db.update({key: Value(data[key], ex) for key in set_keys})
        return len(set_keys)

    def delete(self, *keys: str) -> int:
        '''删除一个或多个通过 ``keys`` 指定的键

        Returns:
            int: 成功操作的数量
        '''
        if len(keys) == 1:
            try:
                self._db.pop(keys[0])
                return 1
            except KeyError:
                return 0
        return self._delete_many(keys)

    def _delete_many(self, keys: Collection[str]):
        per_db = self._db
        if len(self._db) > len(keys) * 10:  # 少量数据
            for key in keys:
                per_db.pop(key)
            return len(keys)
        else:
            alive_keys = set(per_db).difference(keys)
            self._db = {key: per_db[key] for key in alive_keys}
            return len(per_db) - len(alive_keys)

    ## TODO: 接受多个key
    def exists(self, key: str) -> bool:
        '''判断指定的 ``key`` 是否存在或失效

        Args:
            key (str): 指定的键

        Returns:
            bool: 存在状态
        '''
        return self.get(key) is not None  # 失效或不存在都认为不存在

    def keys(self) -> List[str]:
        '''获取所有合法的键

        Returns:
            List[str]: 由键组成的列表
        '''
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
        '''获取指定键的 TTL

        合法的 TTL 仅为非负值，但规定：
        - -1 表示该键永远有效
        - -2 表示该键不存在或已经失效

        Args:
            key (str): 指定的键

        Returns:
            int: 指定键的 TTL， 或特殊情况的规定值
        '''
        value = self._get(key)
        if value is NOT_EXISTS:
            return -2
        return value.ttl

    def incr(self, key: str, amount: int = 1, ex: Optional[int] = None) -> int:
        '''自增，仅在 ``key`` 的值类型为 int 时有效

        Args:
            key (str): 指定的键
            amount (int, optional): 增加的值，默认为 1
            ex (int, optional): 失效时间，默认为 None，表示永远有效

        Raises:
            ValueError: 指定键的值非 int 类型时引发

        Returns:
            int: 操作后的值
        '''
        if not isinstance(amount, int):
            raise ValueError('can not increment by type: %s' % type(amount))
        return self._cre(key, amount, ex)

    def decr(self, key: str, amount: int = 1, ex: Optional[int] = None) -> int:
        '''自减，仅在 ``key`` 的值类型为 int 时有效

        Args:
            key (str): 指定的键
            amount (int, optional): 减少的值，默认为 1
            ex (int, optional): 失效时间，默认为 None，表示永远有效

        Raises:
            ValueError: 指定键的值非 int 类型时引发

        Returns:
            int: 操作后的值
        '''
        if not isinstance(amount, int):
            raise ValueError('can not decrement by type: %s' % type(amount))
        return self._cre(key, -amount, ex)

    def _cre(self, key: str, amount: int, ex: Union[int, None]) -> int:
        val = self._get(key)
        if val is NOT_EXISTS:  # key 失效或不存在
            if ex is None:
                ex = self.default_timeout
            self._db[key] = Value(0, ex)
        elif ex is not None:  # key 存在，但需要重设失效时间
            self._db[key] = Value(val.value, ex)
        return self._db[key].cre(amount)

    def flushdb(self):
        '''清除所有存入的键'''
        self._db.clear()
