# -*- coding: utf-8 -*-

from unittest import TestCase

from pydis.multithreading.connection import open_connection
from pydis.exceptions import ReceiveTimeout


class TestConnection(TestCase):
    def test_base_send_recv(self):
        data = ('#TEST', 'test', None)
        conn1, conn2 = open_connection()
        conn1.send(data)
        self.assertIs(conn2.recv(), data)

    def test_recv_block_timeout(self):
        conn1, _ = open_connection()
        with self.assertRaises(ReceiveTimeout):
            conn1.recv(block=False)
        with self.assertRaises(ReceiveTimeout):
            conn1.recv(timeout=.1)

    def test_close(self):
        conn1, conn2 = open_connection()
        conn1.close()
        self.assertIs(conn2.closed, True)
