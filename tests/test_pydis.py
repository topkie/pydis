# -*- coding: utf-8 -*-

from datetime import timedelta
import time
import unittest

from pydis import Pydis


class Test(unittest.TestCase):
    def test_set_get(self):
        p = Pydis()
        key, val = 'key', 'val'
        p.set(key, val)
        self.assertEqual(p.get(key), val)

    def test_timeout(self):
        p = Pydis()
        key, val, ttl = 'key', 'val', 1
        p.set(key, val, ttl)
        time.sleep(ttl)
        self.assertIsNone(p.get(key))
        ttl = timedelta(seconds=1)
        time.sleep(ttl.total_seconds())
        self.assertIsNone(p.get(key))

    def test_default_timeout(self):
        p = Pydis(2)
        print(p.default_timeout)
        key1, val1, key2, val2 = 'key1', 'val1', 'key2', 'val2'
        p.set(key1, val1)
        p.default_timeout = 1
        p.set(key2, val2)
        self.assertEqual(p.get(key1), val1)
        self.assertEqual(p.get(key2), val2)
        time.sleep(1)
        self.assertEqual(p.get(key1), val1)
        self.assertIsNone(p.get(key2))
        time.sleep(1)
        self.assertIsNone(p.get(key1))
        self.assertIsNone(p.get(key2))

    def test_setnx(self):
        p = Pydis()
        key1, val1, key2, val2 = 'key1', 'val1', 'key2', 'val2'
        p.set(key1, val1)
        self.assertIs(p.setnx(key1, val2), False)
        self.assertIs(p.setnx(key2, val2), True)
        self.assertEqual(p.get(key1), val1)

    def test_delete(self):
        p = Pydis()
        key, val = 'key', 'val'
        p.set(key, val)
        self.assertIs(p.delete(key), True)
        self.assertIsNone(p.get(key))
        self.assertIs(p.delete(key), False)

    def test_exists(self):
        p = Pydis()
        key, val, fake_key = 'key', 'val', 'fake_key'
        p.set(key, val)
        self.assertIs(p.exists(key), True)
        self.assertIs(p.exists(fake_key), False)

    def test_keys(self):
        p = Pydis()
        keys = ['key1', 'key2', 'key3', 'key4', 'key5']
        timeout_keys = ['tk1', 'tk2', 'tk3', 'tk4', 'tk5']
        for key in keys:
            p.set(key, 'val')
        for key in timeout_keys:
            p.set(key, 'val', 1)
        self.assertEqual(p.keys(), keys + timeout_keys)
        time.sleep(1)
        self.assertFalse(set(p.keys()).difference(keys))

    def test_ttl(self):
        p = Pydis()
        key, val, ttl = 'key', 'val', 1
        p.set(key, val, ttl)
        self.assertLessEqual(p.ttl(key), ttl,
                             'alive key\'s TTL should <= %d' % ttl)
        time.sleep(1)
        self.assertEqual(p.ttl(key), 0, 'expired key shoud return 0')
        p.set(key, val)
        self.assertEqual(p.ttl(key), -1, 'non-exoired key return -1')

    def test_flushdb(self):
        p = Pydis()
        keys = ['key1', 'key2', 'key3', 'key4', 'key5']
        for key in keys:
            p.set(key, 'val')
        p.flushdb()
        self.assertFalse(p._db)

    @unittest.skip('TODO')
    def test_empty(self):
        p = Pydis()
        self.assertIs(p.empty, True)
        p.set('key', 'val')
        self.assertIs(p.empty, False)
        p.flushdb()
        self.assertIs(p.empty, True)

    def tearDown(self):
        # Pydis 为单例类，测试完成后需要恢复改动
        Pydis._Singleton__instance = None  # type: ignore
