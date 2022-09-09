# -*- coding: utf-8 -*-

from random import sample as random_sample
from select import select
from threading import Event, Lock, Thread, Condition
from time import monotonic as time
from typing import Generic, TypeVar

from ..core import Core
from ..exceptions import ConnectionClosedError, ReceiveTimeout, ServerStopped
from .connection import Connection, open_connection
from .message import message
from .typing import RequestT

T = TypeVar('T')


class Set(set, Generic[T]):
    '''扩充 set 的功能'''

    def __init__(self, *args, **kwargs):
        self.mutex = Lock()
        self.not_empty = Condition(self.mutex)
        super().__init__(*args, **kwargs)

    def wait(self, block=True, timeout=None) -> bool:
        '''当集合为空时，等待元素被放入集合

        设置了 ``timeout`` 后，如果超时后集合仍为空，返回 False

        如果 ``block`` 被设为 True，``timeout`` 将被忽略

        Args:
            block (bool, optional): 是否阻塞，默认为 True. Defaults to True.
            timeout (Union[int, float], optional): 超时时间，默认为 None，表示永远等待

        Raises:
            ValueError: 当 ``timeout`` 为负值时引发

        Returns:
            bool: 集合是否为空
        '''
        with self.not_empty:
            if not block:
                if not self._qsize():
                    return False
                else:
                    return True
            elif timeout is None:
                while not self._qsize():
                    return self.not_empty.wait()
                else:
                    return True
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        return False
                    return self.not_empty.wait(timeout=remaining)
                else:
                    return True

    def _qsize(self):
        return len(self)

    def remove(self, __element: T):
        '''从集合中移除给定元素，如果元素不存在，不会有任何影响'''
        with self.not_empty:
            return self.discard(__element)

    def add(self, __element: T):
        with self.not_empty:
            super().add(__element)
            self.not_empty.notify()


LOOKUPS_PER_LOOP = 20  # 每轮抽查的键的数量
ACCEPTABLE_STALE = 10  # 10%
TIME_PERC = 25 / 1000  # 25ms
MAX_TIME_SPAN = 0.1    # 100ms


class Server(Core):
    '''用于处理数据的服务'''

    _connections: Set[Connection] = Set()
    _mutex = Lock()
    _stop_evt = Event()
    _started = False

    def __init__(self):
        self.stat_expired_stale_perc = 0
        '''估计的失效键比例，去 % 的整数值'''
        self.last_time_cycle = 0
        '''上次执行清理的时刻'''
        super().__init__()

    @classmethod
    def open_connection(cls) -> Connection:
        with cls._mutex:
            if cls._stop_evt.is_set():
                raise ServerStopped
            ret, conn = open_connection()
            cls._connections.add(conn)
            return ret

    def start(self):
        if not self._started:
            with self._mutex:
                if not self._started:
                    self._stop_evt.clear()
                    server = Thread(target=self._run_server)
                    server.daemon = True
                    server.start()
                    self._started = True

    def _run_server(self):
        try:
            self.serve_forever()
        finally:
            # server 线程因为意外退出
            # 通知客户端连接已经被关闭
            self._close_connections()

    def serve_forever(self):
        while not self._stop_evt.is_set():
            self.active_expire_cycle()
            if not self._connections.wait(timeout=1):
                continue
            conns, *_ = select(self._connections.copy(), [], [], 1)
            for c in conns:
                if c.closed:
                    self._connections.remove(c)
                    continue
                self.handle_request(c)
        else:
            self._close_connections()

    def handle_request(self, c: Connection):
        try:
            msg: RequestT = c.recv(block=False)  # type: ignore
        except (ReceiveTimeout, ConnectionClosedError):
            return
        kind, name, value = msg
        try:
            attr = getattr(self, name)
        except Exception as e:
            resp = (message.ERROR, e)
        else:
            if kind == message.CALL:
                args, kwargs = value  # type: ignore
                try:
                    ret = attr(*args, **kwargs)
                except Exception as e:
                    resp = (message.ERROR, e)
                else:
                    resp = (message.RETURN, ret)
            elif kind == message.GET:
                resp = (message.RETURN, attr)
            elif kind == message.SET:
                setattr(self, name, value)
                resp = (message.RETURN, None)
            else:
                resp = (
                    message.ERROR,
                    TypeError('message kind nuknown')
                )
        c.send(resp)

    def active_expire_cycle(self):
        '''对 redis 定期过期的拙劣模仿'''
        last_time_cycle = self.last_time_cycle
        stat_expired_stale_perc = self.stat_expired_stale_perc
        db = self._db
        expiry_keys = self._expiry_key

        # 没有关联了失效时长的键，所有的键
        # 都不会过期，因此无需清理
        if not expiry_keys:
            return

        start = time()
        # 当预估的失效键占比可接受，或者没有
        # 到清理周期时，不会执行清理
        if (start - last_time_cycle < MAX_TIME_SPAN or
                stat_expired_stale_perc < ACCEPTABLE_STALE):
            return

        # 本次每轮需要抽查的键的数量
        num = len(expiry_keys)
        if num > LOOKUPS_PER_LOOP:
            num = LOOKUPS_PER_LOOP

        last_time_cycle = start
        timelimit = TIME_PERC
        timelimit_exit = False
        total_expired = total_sample = 0
        expired = sample = 0

        while not timelimit_exit and (
            sample == 0 or  # 第一次抽查
            100 * expired / sample > ACCEPTABLE_STALE  # 本轮失效的键太多
        ):
            expired = sample = 0

            sample_keys = random_sample(expiry_keys, num)
            while time() - start < timelimit:
                key = sample_keys.pop()
                if db[key].expired:
                    db.pop(key)
                    expiry_keys.discard(key)
                    expired += 1
                sample += 1
            else:
                timelimit_exit = True

            total_expired += expired
            total_sample += sample

        # 评估失效键比例，本次失效占比 20%，历次占比 80%。当某时刻
        # 有大量的键失效导致清理过程因为超时而退出时，评估值将会增大，
        # 下次将一定会执行清理。我们认为 50% 的键过期即为大量过期，
        # 在历次失效比例为 0 的情况下，经过计算，选择了上述占比
        # 计算方式：
        # 假设本次占比为 x，则历次占比为 (1-x)，本次比例 a, 历次比例 b,
        # 则有 ax + b(1-x) >= 10 => x >= (10-b)/(a-b)，
        # 令 a=0.5, b=0，可解 x>=0.2
        if total_sample:
            self.stat_expired_stale_perc = (
                total_expired / total_sample * 20
                + stat_expired_stale_perc * 0.80
            )

        self.last_time_cycle = last_time_cycle

    @classmethod
    def stop(cls):
        '''停止服务线程'''
        with cls._mutex:
            cls._stop_evt.set()
            cls._started = False

    @classmethod
    def stopped(cls):
        '''返回服务线程是否被关闭'''
        return cls._stop_evt.is_set()

    def _close_connections(self):
        with self._mutex:
            while self._connections:
                conn = self._connections.pop()
                try:
                    conn.close()
                except:
                    pass
