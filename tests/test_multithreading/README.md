## 架构
常见的 C/S 结构，分为 Server、Client、Connection

1. Server：
   - 使用单独的线程和 Pydis 实例管理数据
   - 与 Client 一对多通信，轮询调度
2. Client：
   - 存在于用户线程中，与 Server 通信，代理用户操作
3. Connection
   - 负责 Server 与 Client 的通信

## 实现方式
1. Server
   1. 一个集合保存所有与 Client 通信的 Connection
   2. 执行定期清理
   3. 通过 select.select 阻塞获取由消息的 Connection
   4. 轮询每个有消息的 Connection，非阻塞的获取消息
   5. 如果没有消息，返回 2
   6. 处理并发送结果
   7. 重复 2-6
2. Client
   1. 实现接口，代理操作

## 通信协议
1. 种类：
   1. 命令，为客户端向服务端发送，三元组构成，第一位为消息种类，第二位为命令，第三位为执行命令需要的数据，如
   
      (MessageKind, Command, Any)

      eg:
         - (CALL, 'set',   (('key', 'val', 10), {}))
         - (GET,  'empty', None)
         - (SET, 'name', value)
   2. 结果，服务端向客户端发送，二元组构成，第一位为消息种类，第二位为数据
      
      eg:
         - (RETURN, value)
         - (ERROR, error)
2. MessageKind: 消息种类，对命令种类的抽象
   1. 格式：以 '' 开头的字符串
   2. 种类：
      1. client -> Server
         1. CALL: 调用一个方法
         2. GET: 获取一个 variable, proprety，此命令第三位为空
         3. SET: 设置一个 variable, proprety
      2. server -> client
         1. RETURN: 客户端命令的结果
         2. ERROR: 执行客户端命令时发生的错误

|      | send               | RETURN           | ERROR          |
| ---- | ------------------ | ---------------- | -------------- |
| CALL | (CALL, name, args) | (RETURN, result) | (ERROR, error) |
| GET  | (GET, name, None)  | (RETURN, value)  | (ERROR, error) |
| SET  | (SET, name, value) | (RETURN, None)   | (ERROR, error) |