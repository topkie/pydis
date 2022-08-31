# Pydis 

![Python-version](https://img.shields.io/badge/python-3.6+-blue)

基于 Python dict 的内存管理工具，实现了部分 redis 接口，并在保持 Python 风格的前提下尽量与 redis 保持特性相似。

## 使用

### 开始

```python3
>>> import time
>>> from pydis import Pydis
>>> manager = Pydis()
>>> manager.set('key', 'value')
True
>>> manager.set('key1', 'value1', ex=3)
True
>>> time.sleep(3)
>>> manager.get('key')
'value'
>>> manager.get('key1')  # None
>>> manager.delete('key')
1
>>> mamager.get('key')  # None
```

### setnx

如果不存在给定键则存入，否则不进行任何操作

```python3
>>> manager.setnx('new-key', 'val')
True
>>> manager.setnx('new-key', 'value')
False
>>> manager.get('new-key')
'val'
```

### 判断一个键是否存在

```python3
>>> manager.set('key')
True
>>> manager.exists('key')
True
>>> manager.exists('key1')
False
```

### 获取所有合法的键

```python3
>>> manager.set('key1', 'value')
True
>>> manager.keys()
['key', 'key1']
```

### 获取和设置键的到期时长（秒）

```python3
>>> manager.ttl('key')
-1  # 永久有效
>>> manager.expire('key', 3)
True
>>> time.sleep(3)
>>> manager.get('key')  # None
```

### incr 和 decr

与 redis 特性相同，即如果传入的 key 不存在，则初始化为 0 后进行操作

```python3
>>> manager.incr('key')
1
>>> manager.decr('key')
0
>>> manager.incr('key', amount=3)
3
```

### 清除所有数据

```python3
>>> mamager.flushdb()
True
```
