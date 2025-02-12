# -*- coding: utf-8 -*-

import time
import unittest
from datetime import timedelta

import pydis
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
        p = Pydis()
        pydis.core.default_timeout = 2
        key1, val1, key2, val2 = 'key1', 'val1', 'key2', 'val2'
        p.set(key1, val1)
        pydis.core.default_timeout = 1
        p.set(key2, val2)
        self.assertEqual(p.get(key1), val1)
        self.assertEqual(p.get(key2), val2)
        time.sleep(1)
        self.assertEqual(p.get(key1), val1)
        self.assertIsNone(p.get(key2))
        time.sleep(1)
        self.assertIsNone(p.get(key1))
        self.assertIsNone(p.get(key2))
        pydis.core.default_timeout = None

    def test_setnx(self):
        p = Pydis()
        key1, val1, key2, val2 = 'key1', 'val1', 'key2', 'val2'
        p.set(key1, val1)
        self.assertIs(p.setnx(key1, val2), False)
        self.assertIs(p.setnx(key2, val2), True)
        self.assertEqual(p.get(key1), val1)

    def test_mget(self):
        p = Pydis()
        keys = ['key1', 'key2', 'key3', 'key4', 'key5']
        vals = ['val1', 'val2', 'val3', 'val4', 'val5']
        for key, val in zip(keys, vals):
            p.set(key, val)
        self.assertFalse(set(p.mget(keys[:3])).difference(vals[:3]))

    def test_mset(self):
        p = Pydis()
        self.assertIs(p.mset({'key%d' % i: 'val%d' % i for i in range(5)}), True)
        self.assertEqual(len(p.keys()), 5)
        self.assertIs(p.mset({'key%d' % i: 'val%d' % i for i in range(3, 8)}), True)
        self.assertEqual(len(p.keys()), 8)

    def test_msetnx(self):
        p = Pydis()
        data1 = {'key%d' % i: 'val%d' % i for i in range(5)}
        data2 = {'key%d' % i: 'val%d' % i for i in range(3, 8)}
        self.assertEqual(p.msetnx(data1), 5)
        self.assertEqual(len(p.keys()), 5)
        self.assertEqual(p.msetnx(data2), 3)
        self.assertEqual(len(p.keys()), 8)

    def test_delete(self):
        p = Pydis()
        key1, val1, key2, val2 = 'key1', 'val1', 'key2', 'val2'
        p.set(key1, val1)
        p.set(key2, val2)
        self.assertEqual(p.delete(key1), 1)
        self.assertIsNone(p.get(key1))
        self.assertEqual(p.delete(key1), 0)
        p.set(key1, val1)
        self.assertEqual(p.delete(key1, key2), 2)

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
        self.assertEqual(p.ttl(key), -2, 'expired key shoud return -2')
        p.set(key, val)
        self.assertEqual(p.ttl(key), -1, 'non-exoired key return -1')

    def test_flushdb(self):
        p = Pydis()
        keys = ['key1', 'key2', 'key3', 'key4', 'key5']
        for key in keys:
            p.set(key, 'val')
        p.flushdb()
        self.assertFalse(p._db)

    def test_empty(self):
        p = Pydis()
        self.assertIs(p.empty, True)
        p.set('key', 'val')
        self.assertIs(p.empty, False)
        p.flushdb()
        self.assertIs(p.empty, True)

    def test_incr_decr(self):
        p = Pydis()
        key, amount, ttl = 'key', 3, 1
        self.assertEqual(p.incr(key, amount, ttl), amount)
        self.assertEqual(p.incr(key), amount + 1)
        self.assertEqual(p.get(key), amount + 1)
        time.sleep(ttl)
        self.assertEqual(p.decr(key), -1)
        self.assertEqual(p.decr(key, amount, ttl), -(amount + 1))
        time.sleep(ttl)
        self.assertIsNone(p.get(key))

    def test_expire(self):
        p = Pydis()
        key, val = 'key', 'val'
        with self.assertRaises(ValueError):
            p.expire(key, 1, nx=True, xx=True)
        self.assertIs(p.expire(key, 1), False)
        p.set(key, val, 10)
        self.assertIs(p.expire(key, 1, nx=True), False)
        p.set(key, val)
        self.assertIs(p.expire(key, 1, xx=True), False)

        self.assertIs(p.expire(key, 0), True)
        self.assertIsNone(p.get(key))

    def tearDown(self):
        # Pydis 为单例类，测试完成后需要恢复改动
        Pydis._Singleton__instance = None  # type: ignore
