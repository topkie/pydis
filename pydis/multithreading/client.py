# -*- coding: utf-8 -*-

from datetime import timedelta
from functools import wraps
from typing import Any, Collection, Dict, List, Optional, Union

from .server import Server
from .typing import RequestT, ResponseT
from .message import message


class Client:
    def __init__(self) -> None:
        self._conn = Server.open_connection()
        Server().start()

    def close(self):
        self._conn.close()

    def __del__(self):
        self.close()

    def execute_command(
        self,
        msg: RequestT,
        block=True,
        timeout: Optional[float] = None
    ) -> ResponseT:
        self._conn.send(msg)
        return self._conn.recv(block, timeout)  # type: ignore


def general_response_handler(func):
    @wraps(func)
    def wraper(self, *args, **kwargs):
        kind, result = func(self, *args, **kwargs)
        if kind == message.RETURN:
            return result
        elif kind == message.ERROR:
            raise result
        else:
            raise ValueError('message kind nuknown')
    return wraper


def make_message(kind, name, *args, **kwargs):
    return (kind, name, (args, kwargs))


class PydisClient(Client):
    '''基于 dict 的内存管理工具
    
    将用户操作转交给后台服务线程执行，并返回结果

    可通过改变 ``default_timeout`` 属性改变`本实例`的默认失效时长，
    新的失效时长只对新存入的键有效，改变前存入的键不受影响

    每个方法都含有 ``block`` 和 ``timeout`` 参数，它们的用法和作用
    都与 queue.Queue.get 相同，但超时时会由底层连接引发 ReceiveTimeout
    异常，这个异常在 ``pydis.exceptions`` 中定义

    线程不安全，不要在线程间共享

    Attributes:
        default_timeout (float): 
            本实例的默认失效时长，默认为 None，表示永远有效
    '''

    def __init__(
        self,
        default_timout: Optional[Union[float, timedelta]] = None
    ) -> None:
        self.default_timout = default_timout
        super().__init__()

    @general_response_handler
    def decr(
        self,
        key: str,
        amount: int = 1,
        ex: Optional[Union[float, timedelta]] = None,
        block=True, timeout: Optional[float] = None
    ) -> int:
        '''自减，仅在 ``key`` 的值类型为 int 时有效

        Args:
            key (str): 指定的键
            amount (int, optional): 减少的值，默认为 1
            ex (float, optional): 失效时长，默认为 None，表示永远有效

        Raises:
            ValueError: 指定键的值非 int 类型时引发

        Returns:
            int: 操作后的值
        '''
        if ex is None:
            ex = self.default_timout

        msg = make_message(
            message.CALL,
            'decr',
            key, amount=amount, ex=ex
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def delete(
        self,
        *keys: str,
        block=True, timeout: Optional[float] = None
    ) -> int:
        '''删除一个或多个通过 ``keys`` 指定的键

        Returns:
            int: 成功操作的数量
        '''
        msg = make_message(
            message.CALL,
            'delete',
            *keys
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @property
    @general_response_handler
    def empty(self):
        msg = (message.GET, 'empty', None)
        return self.execute_command(msg, block=False)  # type: ignore

    @general_response_handler
    def exists(
        self,
        key: str,
        block=True, timeout: Optional[float] = None
    ) -> bool:
        '''判断指定的 ``key`` 是否存在或失效

        Args:
            key (str): 指定的键

        Returns:
            bool: 存在状态
        '''
        msg = make_message(message.CALL, 'exists', key)
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def expire(
        self,
        key: str,
        time: Union[int, timedelta],
        nx: bool = False,
        xx: bool = False,
        block=True, timeout: Optional[float] = None
    ) -> bool:
        '''将 ``key`` 的失效时长设为 ``time``（秒）

        ``nx``: 只有指定键不会失效时才进行操作，默认为 False

        ``xx``: 只有指定键会失效时才进行操作，默认为 False

        Args:
            key (str): 指定的键
            time (Union[int, timedelta]): 失效时长，可接受 int 和 timedelta 类型
            nx (bool, optional): 不存在则， 默认为 False
            xx (bool, optional): 存在则， 默认为 False

        Raises:
            TypeError: ``nx`` 和 ``xx`` 同时为 True 时引发

        Returns:
            bool: 操作是否成功
        '''
        msg = make_message(
            message.CALL,
            'expire',
            key, time, nx=nx, xx=xx
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def flushdb(self, block=True, timeout: Optional[float] = None):
        '''清除所有存入的键'''
        msg = make_message(message.CALL, 'flushdb')
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def get(
        self,
        key: str,
        block=True, timeout: Optional[float] = None
    ) -> Union[Any, None]:
        '''获取指定 key 的值

        当传入的 key 不存在或失效时返回 None.

        Args:
            key (str): 用于取值的 key.

        Returns:
            Union[Any, None]: key 对于的值，不存在或失效为 None
        '''
        msg = make_message(message.CALL, 'get', key)
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def incr(
        self,
        key: str,
        amount: int = 1,
        ex: Optional[Union[float, timedelta]] = None,
        block=True, timeout: Optional[float] = None
    ) -> int:
        '''自增，仅在 ``key`` 的值类型为 int 时有效

        Args:
            key (str): 指定的键
            amount (int, optional): 增加的值，默认为 1
            ex (float, optional): 失效时长，默认为 None，表示永远有效

        Raises:
            ValueError: 指定键的值非 int 类型时引发

        Returns:
            int: 操作后的值
        '''
        if ex is None:
            ex = self.default_timout
        msg = make_message(
            message.CALL,
            'incr',
            key, amount=amount, ex=ex
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def keys(
            self,
            block=True, timeout: Optional[float] = None) -> List[str]:
        '''获取所有合法的键

        Returns:
            List[str]: 由键组成的列表
        '''
        msg = make_message(message.CALL, 'keys')
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def mget(
        self,
        keys: Collection[str],
        block=True, timeout: Optional[float] = None
    ) -> List[Any]:
        '''获取通过 ``keys`` 指定的键的值

        返回一个列表，长度与 ``keys`` 的长度相等，值的位置
        与 ``keys`` 中键的位置一一对应，不存在或失效的键其值用 None 填充

        Args:
            keys (Collection[str]): 键的集合

        Returns:
            List[Any]: 与 ``keys`` 中的键对应的值，不存在的用 None 填充
        '''
        msg = make_message(
            message.CALL,
            'mget',
            keys
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def mset(
        self,
        data: Dict[str, Any],
        ex: Optional[Union[float, timedelta]] = None,
        block=True, timeout: Optional[float] = None
    ) -> bool:
        '''存入通过 ``data`` 指定的键值对。

        ``data`` 需为 dict 类型。

        ``ex`` 用于指定失效时长，可接受 int、float 和 timedelta 类型

        本操作不会失败，因此返回值恒为 True

        Args:
            data (Dict[str, Any]): 待存入的键值对
            ex (Union[int, timedelta], optional): 失效时长. 默认为 None

        Returns:
            bool: True
        '''
        if ex is None:
            ex = self.default_timout
        msg = make_message(
            message.CALL,
            'mset',
            data, ex=ex
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def msetnx(
        self,
        data: Dict[str, Any],
        ex: Optional[Union[float, timedelta]] = None,
        block=True, timeout: Optional[float] = None
    ) -> int:
        '''存入通过 dict 指定的多个键值对

        与 redis 不同的是，如果传入本方法的部分 key 已经存在，
        这些 key 将会被忽略， pydis 会将剩下键值对存起来。

        Args:
            data (Dict[str, Any]): 待存储的键值对
            ex (Union[float, timedelta], optional): 失效时长. 默认为 None.

        Returns:
            int: 成功存储的键值对的数量
        '''
        if ex is None:
            ex = self.default_timout
        msg = make_message(
            message.CALL,
            'msetnx',
            data, ex=ex
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def set(
            self,
            key: str,
            value: Any,
            ex: Optional[Union[float, timedelta]] = None,
            block=True, timeout: Optional[float] = None
    ) -> bool:
        '''将 ``key`` 的值设为 ``value``，``value`` 不能为 None

        ex 用于指定失效时长，可接受 int、float 和 timedelta 类型，
        默认为 None 表示永远有效

        本操作不会失败，因此返回值恒为 True

        Args:
            key (str): 指定的 key
            value (Any): 待设定的值
            ex (Union[int, timedelta], optional): 失效时长. 默认为 None

        Raises:
            ValueError: 传入的 ``value`` 为 None 时引发

        Returns:
            bool: True
        '''
        if ex is None:
            ex = self.default_timout
        msg = make_message(
            message.CALL,
            'set',
            key, value, ex=ex)
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def setnx(
        self,
        key: str,
        value: Any,
        ex: Optional[Union[float, timedelta]] = None,
        block=True, timeout: Optional[float] = None
    ) -> bool:
        '''当 ``key`` 不存在时，将 ``key`` 的值设为 ``value``

        Args:
            key (str): 待设定的键
            value (Any): 待设定的值
            ex (Union[float, timedelta], optional): 失效时长. 默认为 None

        Returns:
            bool: 操作是否成功
        '''
        if ex is None:
            ex = self.default_timout
        msg = make_message(
            message.CALL,
            'setnx',
            key, value, ex=ex
        )
        return self.execute_command(msg, block, timeout)  # type: ignore

    @general_response_handler
    def ttl(
        self,
        key: str,
        block=True, timeout: Optional[float] = None
    ) -> int:
        '''获取指定键的 TTL

        合法的 TTL 仅为非负值，但规定：
        - -1 表示该键永远有效
        - -2 表示该键不存在或已经失效

        Args:
            key (str): 指定的键

        Returns:
            int: 指定键的 TTL， 或特殊情况的规定值
        '''
        msg = make_message(message.CALL, 'ttl', key)
        return self.execute_command(msg, block, timeout)  # type: ignore
