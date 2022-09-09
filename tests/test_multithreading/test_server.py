# -*- coding: utf-8 -*-

from unittest import TestCase
from unittest.mock import Mock, patch

from pydis.multithreading.server import Set, Server


class TestSet(TestCase):

    def test_wait_empty_queue(self):
        from threading import Event, Thread
        from time import time

        def test_step(step, s, time_wait):
            # block=False
            # pass
            # block=True, timeout=None
            step.wait()
            step.clear()
            step.wait(time_wait)
            s.add(None)
            # block=True, timeout < 0
            # pass
            # block=True, timeout = 0
            # pass
            # block = True, timeout > 0
            # pass

        step = Event()
        s = Set()
        time_wait = 0.1
        Thread(target=test_step, args=(step, s, time_wait), daemon=True).start()
        # block=False
        # step.set()
        self.assertIs(s.wait(block=False), False)
        # block=True, timeout=None
        step.set()
        start = time()
        self.assertIs(s.wait(), True)
        self.assertTrue((time() - start - time_wait) < 0.1)
        s.remove(None)
        # block=True, timeout < 0
        # step.set()
        with self.assertRaises(ValueError):
            s.wait(timeout=-1)
        # block=True, timeout = 0
        self.assertIs(s.wait(timeout=0), False)
        # block = True, timeout > 0
        start = time()
        self.assertIs(s.wait(timeout=time_wait), False)
        self.assertTrue((time() - start - time_wait) < 0.1)

    def test_wait_not_empty(self):
        from time import time
        q = Set()
        q.add(None)
        # block=True, timeout=None
        self.assertIs(q.wait(), True)
        # block=True, timeout<0
        with self.assertRaises(ValueError):
            q.wait(timeout=-1)
        # block=True, timeout>=0
        start = time()
        self.assertIs(q.wait(timeout=0), True)
        self.assertIs(q.wait(timeout=1), True)
        self.assertTrue(time() - start < 0.001)
        # block=False, timeout=any
        self.assertIs(q.wait(block=False), True)

    def test_wait_add_halfway(self):
        from threading import Event, Thread
        from time import time

        def add_thread(s, evt):
            evt.wait(0.1)
            s.add(None)
        s = Set()
        e = Event()
        t = Thread(target=add_thread, args=(s, e), daemon=True)
        t.start()
        start = time()
        self.assertIs(s.wait(), True)
        self.assertTrue(time() - start - 0.1 < 0.1)
        t.join()
        s.pop()

        t = Thread(target=add_thread, args=(s, e), daemon=True)
        t.start()
        start = time()
        self.assertIs(s.wait(timeout=0.5), True)
        self.assertTrue(time() - start - 0.1 < 0.1)


class TestServer(TestCase):

    def setUp(self):
        Server._Singleton__instance = None  # type: ignore
        Server()._close_connections()
        # Server._stop_evt.clear()

    def test_open_connection(self):
        Server.open_connection()
        self.assertEqual(Server._connections._qsize(), 1)

    @patch.object(Server._stop_evt, 'is_set', return_value=True)
    def test_open_connection_server_stopped(self, _):
        from pydis.exceptions import ServerStopped
        with self.assertRaises(ServerStopped):
            Server.open_connection()

    def test_start(self):
        from threading import get_ident

        def _fack_run_server(server):
            server._connections.add(get_ident())
        Server._run_server = _fack_run_server  # type: ignore
        Server().start()
        self.assertNotEqual(Server._connections.pop(), get_ident())

    @patch.object(Server, 'serve_forever')
    @patch.object(Server, '_close_connections')
    def test_run_server(self, mk_serve_forever, mk_close_connections):
        Server()._run_server()
        mk_serve_forever.assert_called()
        mk_close_connections.assert_called()

    @patch.object(Server, 'serve_forever', side_effect=Exception('ops'))
    @patch.object(Server, '_close_connections')
    def test_run_server_exception(self, mk_serve_forever, mk_close_connections):
        with self.assertRaises(Exception):
            Server()._run_server()
        mk_serve_forever.assert_called()
        mk_close_connections.assert_called()

    @patch.object(Server._stop_evt, 'is_set', return_value=True)
    @patch.object(Server, 'active_expire_cycle')
    @patch.object(Server, '_close_connections')
    def test_server_forever_stoped(self, close_func, active_func, _):
        Server().serve_forever()
        close_func.assert_called()
        active_func.assert_not_called()

    @patch.object(Server, 'active_expire_cycle')
    @patch.object(Server, '_close_connections')
    @patch('pydis.multithreading.server.select')
    def test_server_forever_wait_for_conn(self, select, *mock_funcs):
        from time import time
        with patch.object(Server._stop_evt, 'is_set', Mock(side_effect=[False, True])):
            start = time()
            Server().serve_forever()
        self.assertLess(time() - start - 1, 0.1)
        select.assert_not_called()
        for func in mock_funcs:
            func.assert_called()

    @patch.object(Server, 'active_expire_cycle')
    @patch.object(Server, '_close_connections')
    @patch('pydis.multithreading.server.Set.remove')
    @patch.object(Server, 'handle_request')
    def test_server_forever_conn_closed(self, handle_request, *mock_funcs):
        with patch.object(Server._stop_evt, 'is_set', Mock(side_effect=[False, False, True])):
            conn = Server.open_connection()
            conn.close()
            self.assertIs(conn.closed, True)
            Server().serve_forever()
        handle_request.assert_not_called()
        for mock_func in mock_funcs:
            mock_func.assert_called()

    @patch.object(Server, 'active_expire_cycle')
    @patch.object(Server, '_close_connections')
    @patch.object(Server, 'handle_request')
    @patch('pydis.multithreading.server.Set.remove')
    def test_server_forever_conn_num_eq_0(self, remove, handle, *mock_funcs):
        with patch.object(Server._stop_evt, 'is_set', Mock(side_effect=[False, True])):
            Server().serve_forever()
        remove.assert_not_called()
        handle.assert_not_called()
        for mock_func in mock_funcs:
            mock_func.assert_called()

    @patch.object(Server, 'active_expire_cycle')
    @patch.object(Server, '_close_connections')
    @patch.object(Server, 'handle_request')
    @patch('pydis.multithreading.server.Set.remove')
    def test_server_forever_conn_num_eq_1(self, remove, *mock_funcs):
        with patch.object(Server._stop_evt, 'is_set', Mock(side_effect=[False, False, True])):
            c = Server.open_connection()
            # 因为没有另开服务线程，
            # 因此 send 要在 server_forever 前
            c.send('#test')  # type: ignore
            Server().serve_forever()
        remove.assert_not_called()
        for mock_func in mock_funcs:
            mock_func.assert_called()

    @patch.object(Server, 'active_expire_cycle')
    @patch.object(Server, '_close_connections')
    @patch.object(Server, 'handle_request')
    @patch('pydis.multithreading.server.Set.remove')
    def test_server_forever_conn_num_gt_1_like_2(self, remove, handle, *mock_funcs):
        with patch.object(Server._stop_evt, 'is_set', Mock(side_effect=[False, False, False, True])):
            c1 = Server.open_connection()
            c2 = Server.open_connection()
            c1.send('#test')  # type: ignore
            c2.send('#test')  # type: ignore
            Server().serve_forever()
        remove.assert_not_called()
        self.assertEqual(handle.call_count, 2)
        for mock_func in mock_funcs:
            mock_func.assert_called()

    @patch('pydis.multithreading.server.getattr', create=True)
    def test_handle_request_recv_exception_ReceiveTimeout(self, getattr):
        from pydis.multithreading.connection import open_connection
        c, _ = open_connection()
        Server().handle_request(c)
        getattr.assert_not_called()

    @patch('pydis.multithreading.server.getattr', side_effect=ValueError('ops'), create=True)
    def test_handle_request_getattr_exception(self, getattr):
        from pydis.multithreading.connection import open_connection
        from pydis.multithreading.message import message
        c1, c2 = open_connection()
        c1.send(('#test', 'fake', 'fake'))  # type: ignore
        with patch.object(c2, 'send') as send:
            Server().handle_request(c2)
            getattr.assert_called()
            send.assert_called()
            call_args = send.call_args
            args = call_args[0]  # ((message.ERROR, ValueError('ops'),)
            kwargs = call_args[1]  # {}
            self.assertTrue(args)
            self.assertTrue(len(args) == 1)
            self.assertFalse(kwargs)
            kind, err = args[0]
            self.assertEqual(kind, message.ERROR)
            self.assertTrue(isinstance(err, ValueError))

    # 本函数与上个流程一致，只是值有区别
    @patch('pydis.multithreading.server.getattr', return_value=Mock(side_effect=ValueError('ops')), create=True)
    def test_handle_request_kind_call_execute_exception(self, getattr):
        from pydis.multithreading.connection import open_connection
        from pydis.multithreading.message import message
        c1, c2 = open_connection()
        c1.send((message.CALL, 'fake', (('fake_arg',), {})))
        with patch.object(c2, 'send') as send:
            Server().handle_request(c2)
            getattr.assert_called()
            getattr.return_value.assert_called_with('fake_arg')
            send.assert_called()
            call_args = send.call_args
            args = call_args[0]  # ((message.ERROR, ValueError('ops'),)
            kwargs = call_args[1]  # {}
            self.assertTrue(args)
            self.assertTrue(len(args) == 1)
            self.assertFalse(kwargs)
            kind, err = args[0]
            self.assertEqual(kind, message.ERROR)
            self.assertTrue(isinstance(err, ValueError), f'err: {err}')

    # 本函数与上个流程一致，只是值有区别
    @patch('pydis.multithreading.server.getattr', return_value=Mock(return_value='ok'), create=True)
    def test_handle_request_kind_call_return_ok(self, getattr):
        from pydis.multithreading.connection import open_connection
        from pydis.multithreading.message import message
        c1, c2 = open_connection()
        c1.send((message.CALL, 'fake', (('fake_arg',), {})))
        with patch.object(c2, 'send') as send:
            Server().handle_request(c2)
            getattr.assert_called()
            getattr.return_value.assert_called_with('fake_arg')
            send.assert_called()
            call_args = send.call_args
            args = call_args[0]  # ((message.RETURN, 'ok'),)
            kwargs = call_args[1]  # {}
            self.assertTrue(args)
            self.assertTrue(len(args) == 1)
            self.assertFalse(kwargs)
            kind, ret = args[0]
            self.assertEqual(kind, message.RETURN)
            self.assertEqual(ret, 'ok')

    # 本函数与上个流程一致，只是值有区别
    @patch('pydis.multithreading.server.getattr', return_value='ok', create=True)
    def test_handle_request_kind_get_ok(self, getattr):
        from pydis.multithreading.connection import open_connection
        from pydis.multithreading.message import message
        c1, c2 = open_connection()
        c1.send((message.GET, 'fake', (('fake_arg',), {})))
        with patch.object(c2, 'send') as send:
            Server().handle_request(c2)
            getattr.assert_called()
            send.assert_called()
            call_args = send.call_args
            args = call_args[0]  # ((message.RETURN, 'ok'),)
            kwargs = call_args[1]  # {}
            self.assertTrue(args)
            self.assertTrue(len(args) == 1)
            self.assertFalse(kwargs)
            kind, ret = args[0]
            self.assertEqual(kind, message.RETURN)
            self.assertEqual(ret, 'ok')

    # 本函数与上个流程一致，只是值有区别
    @patch('pydis.multithreading.server.setattr', create=True)
    @patch('pydis.multithreading.server.getattr', create=True)
    def test_handle_request_kind_set(self, getattr, setattr):
        from pydis.multithreading.connection import open_connection
        from pydis.multithreading.message import message
        c1, c2 = open_connection()
        c1.send((message.SET, 'fake', 'fake_arg'))
        with patch.object(c2, 'send') as send:
            Server().handle_request(c2)
            getattr.assert_called()
            setattr.assert_called_with(Server(), 'fake', 'fake_arg')
            send.assert_called()
            call_args = send.call_args
            args = call_args[0]  # ((message.RETURN, None),)
            kwargs = call_args[1]  # {}
            self.assertTrue(args)
            self.assertTrue(len(args) == 1)
            self.assertFalse(kwargs)
            kind, ret = args[0]
            self.assertEqual(kind, message.RETURN)
            self.assertIs(ret, None)

    @patch('pydis.multithreading.server.getattr', create=True)
    def test_handle_request_unknown_kind(self, getattr):
        from pydis.multithreading.connection import open_connection
        from pydis.multithreading.message import message
        c1, c2 = open_connection()
        c1.send(('fake_kind', 'fake', 'fake_arg'))  # type: ignore
        with patch.object(c2, 'send') as send:
            Server().handle_request(c2)
            getattr.assert_called()
            send.assert_called()
            call_args = send.call_args
            args = call_args[0]  # ((message.ERROR, TypeError('message kind nuknown')),)
            kwargs = call_args[1]  # {}
            self.assertTrue(args)
            self.assertTrue(len(args) == 1)
            self.assertFalse(kwargs)
            kind, err = args[0]
            self.assertEqual(kind, message.ERROR)
            self.assertTrue(isinstance(err, TypeError))

    def test_stop(self):
        Server.stop()
        self.assertIs(Server.stopped(), True)
        Server._stop_evt.clear()

    def test_stopped(self):
        with patch.object(Server._stop_evt, 'is_set', return_value=True):
            self.assertIs(Server.stopped(), True)
        with patch.object(Server._stop_evt, 'is_set', return_value=False):
            self.assertIs(Server.stopped(), False)

    def test_close_connections(self):
        conns = [Server.open_connection() for _ in range(5)]
        Server()._close_connections()
        self.assertIs(all(conn.closed for conn in conns), True)
