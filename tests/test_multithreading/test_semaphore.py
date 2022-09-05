# -*- coding: utf-8 -*-

from queue import Queue
from threading import Thread
from unittest import TestCase

from pydis.multithreading.semaphore import Semaphore


class TestSemaphore(TestCase):
    def test_incr(self):
        s = Semaphore()
        q = Queue()
        Thread(target=lambda: q.put(s.wait())).start()
        s.incr()
        self.assertIs(q.get(), True)
        self.assertEqual(s._value, 1)

    def test_decr(self):
        s = Semaphore()
        q = Queue()
        s.decr()
        self.assertEqual(s._value, -1)
        Thread(target=lambda: q.put(s.wait())).start()
        s.incr(2)
        self.assertIs(q.get(), True)

    def test_wait(self):
        s = Semaphore()
        s.incr()
        self.assertIs(s.wait(), True)
        s.decr()
        self.assertIs(s.wait(blocking=False), False)
        self.assertIs(s.wait(timeout=.1), False)
