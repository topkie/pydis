# -*- coding: utf-8 -*-

from queue import Empty, Queue
from threading import Event
from typing import Any, Optional, Tuple, Union

from pydis.exceptions import ReceiveTimeout


class Connection:
    '''基于 Queue 抽象的连接端点

    线程不安全，不要在线程间共享
    '''

    def __init__(
        self,
        q_send: Queue,
        q_recv: Queue,
        close_evt: Event
    ):
        self.q_send, self.q_recv = q_send, q_recv
        self._closed = close_evt

    def send(
        self,
        data: Tuple[str, Union[str, None], Union[Any, None]]
    ):
        self.q_send.put(data)

    def recv(
        self,
        block=True,
        timeout: Optional[float] = None
    ) -> Tuple[str, Union[str, None], Union[Any, None]]:
        '''从连接中接收数据

        如果设定了超时，超时时会抛出 Empty

        Args:
            block (bool, optional): _description_. Defaults to True.
            timeout (Optional[float], optional): _description_. Defaults to None.

        Returns:
            Tuple[str, Union[str, None], Union[Any, None]]: _description_
        '''
        try:
            return self.q_recv.get(block, timeout)
        except Empty:
            raise ReceiveTimeout

    def close(self):
        self._closed.set()

    @property
    def closed(self):
        return self._closed.is_set()


def open_connection() -> Tuple[Connection, Connection]:
    '''打开一个连接并返回他的两端

    Returns:
        Tuple[Connection, Connection]: 一个连接的两端
    '''
    q1, q2 = Queue(), Queue()
    evt = Event()
    return Connection(q1, q2, evt), Connection(q2, q1, evt)
