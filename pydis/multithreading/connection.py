# -*- coding: utf-8 -*-

import socket
from queue import Empty, Queue
from threading import Event
from typing import Optional, Tuple

from ..exceptions import ConnectionClosedError, ReceiveTimeout
from .typing import MessageT


CLOSE = '#CLS'


class Connection:
    '''基于 Queue 抽象的连接端点

    线程不安全，不要在线程间共享
    '''

    def __init__(
        self,
        q_send: Queue,
        q_recv: Queue,
        close_evt: Event,
        socket: socket.socket,
    ):
        self.q_send, self.q_recv = q_send, q_recv
        self._closed = close_evt
        socket.setblocking(False)
        self._socket = socket

    def send(self, data: MessageT):
        '''发送数据

        当连接被关闭时，引发 ConnectionClosed 错误。
        这个错误在 pydis.exceptions 中定义
        '''
        if self.closed:
            raise ConnectionClosedError('connection has been closed')
        self.q_send.put(data)
        self._socket.send(b'x')

    def recv(
        self, block=True, timeout: Optional[float] = None
    ) -> MessageT:
        '''从连接中接收数据

        如果设定了超时，超时时会抛出 ReceiveTimeout

        当连接被关闭时，抛出 ConnectionClosedError

        这些错误在 pydis.exceptions 中定义
        '''
        if self.closed:
            raise ConnectionClosedError('connection has been closed')
        try:
            self._socket.recv(1)
        except socket.error:
            pass
        try:
            ret = self.q_recv.get(block, timeout)
            if ret is CLOSE:
                raise ConnectionClosedError('connection has been closed')
            self.q_recv.task_done()
            return ret
        except Empty:
            raise ReceiveTimeout

    def fileno(self):
        return self._socket.fileno()

    def close(self):
        if not self.closed:
            self._closed.set()
            self.q_send.put(CLOSE)
            self._socket.send(b'x')
        self._socket.close()

    def __del__(self):
        self.close()

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
    sock1, sock2 = socket.socketpair()
    return Connection(q1, q2, evt, sock1), Connection(q2, q1, evt, sock2)
