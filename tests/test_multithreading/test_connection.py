# -*- coding: utf-8 -*-

from multiprocessing import Event
from unittest import TestCase

from pydis.exceptions import ConnectionClosedError, ReceiveTimeout
from pydis.multithreading.connection import open_connection


class TestConnection(TestCase):
    def test_base_send_recv(self):
        data = ('#TEST', 'test', None)
        conn1, conn2 = open_connection()
        conn1.send(data)  # type: ignore
        self.assertIs(conn2.recv(), data)

    def test_recv_block_timeout(self):
        conn1, _ = open_connection()
        with self.assertRaises(ReceiveTimeout):
            conn1.recv(block=False)
        with self.assertRaises(ReceiveTimeout):
            conn1.recv(timeout=.1)

    def test_recv_ConnectionClosedError_while_closed(self):
        # 测试 recv 函数在等待数据时连接被关闭
        # 应该抛出 ConnectionClosedError
        from threading import Thread, Event
        from time import sleep

        def close_thread(conn, signal):
            signal.wait()
            sleep(.1)  # 等待另一端准备好接收数据
            conn.close()
        conn1, conn2 = open_connection()
        close_signal = Event()
        Thread(target=close_thread, args=(conn2, close_signal), daemon=True).start()
        with self.assertRaises(ConnectionClosedError):
            close_signal.set()
            conn1.recv()

    def test_close(self):
        conn1, conn2 = open_connection()
        conn1.close()
        self.assertIs(conn2.closed, True)
        with self.assertRaises(ConnectionClosedError):
            conn1.send('test')  # type: ignore
        with self.assertRaises(ConnectionClosedError):
            conn1.recv()

    def test_select_support(self):
        import select
        import threading
        c1, c2 = open_connection()
        c3, c4 = open_connection()
        stop = threading.Event()

        def consumer(conns, stop):
            while not stop.is_set():
                can_read, *_ = select.select(conns, [], [], 0.1)
                for r in can_read:
                    r.send((r, r.recv()))
        t = threading.Thread(target=consumer, args=([c2, c4], stop), daemon=True)
        t.start()
        data = 'test'
        c1.send(data)  # type: ignore
        c, ret = c1.recv()  # type: ignore
        self.assertIs(c, c2)
        self.assertEqual(ret, 'test')
        c3.send(data)  # type: ignore
        c, ret = c3.recv()  # type: ignore
        self.assertIs(c, c4)
        self.assertEqual(ret, 'test')
        stop.set()
        t.join()
