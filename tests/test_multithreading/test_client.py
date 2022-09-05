# -*- coding: utf-8 -*-

from datetime import timedelta
import unittest
from unittest import TestCase, mock

from pydis.multithreading.client import Client, PydisClient
from pydis.multithreading.connection import open_connection
from pydis.utils import Singleton


class FakeServer(metaclass=Singleton):
    @classmethod
    def open_connection(cls):
        cls.conn, conn = open_connection()
        return conn

    def start(self): pass


@mock.patch('pydis.multithreading.client.Server', FakeServer)
class TestClient(TestCase):
    def test_close(self):
        c = Client()
        c.close()
        self.assertIs(FakeServer().conn.closed, True)

    def test_del(self):
        c = Client()
        del c
        self.assertIs(FakeServer().conn.closed, True)

    @unittest.skip('Client.execute_command based on Connection')
    def test_execute_command(self): pass


class TestFunction(TestCase):
    def test_make_message(self):
        from pydis.multithreading.client import make_message
        from pydis.multithreading.message import message
        fake_arg, fake_kwargs = 'fake_arg', {'fake_kw1': 'fake_val1', 'fake_kw2': 'fake_val2'}
        msg = make_message(message.CALL, 'fake_call')
        self.assertEqual(msg, (message.CALL, 'fake_call', (tuple(), {})))
        msg = make_message(message.CALL, 'fake_call', fake_arg, **fake_kwargs)
        self.assertEqual(msg, (message.CALL, 'fake_call', ((fake_arg,), fake_kwargs)))

    def test_general_response_handler(self):
        from pydis.multithreading.client import general_response_handler
        from pydis.multithreading.message import message
        mock_func = mock.Mock(return_value=(message.RETURN, 'ok'))
        handler = general_response_handler(mock_func)
        ret = handler(self, 'fake', fake='fake')
        self.assertEqual(ret, 'ok')
        mock_func = mock.Mock(return_value=(message.ERROR, Exception('error')))
        handler = general_response_handler(mock_func)
        with self.assertRaises(Exception):
            ret = handler(self, 'fake', fake='fake')
        mock_func.assert_called()
        mock_func.assert_called_with(self, 'fake', fake='fake')


class TestPydisClient(TestCase):
    @mock.patch.object(PydisClient, 'execute_command')
    def test_set_arguments(self, execute_command):
        from pydis.multithreading.message import message
        p = PydisClient()
        execute_command.return_value = (message.RETURN, True)
        p.set('fake_key', 'fake_val', 1)
        msg = (message.CALL, 'set', (('fake_key', 'fake_val'), {'ex': 1}))
        execute_command.assert_called_with(msg, True, None)
