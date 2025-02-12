# -*- coding: utf-8 -*-

class ReceiveTimeout(Exception):
    '''从连接中接收数据超时'''


class ConnectionClosedError(Exception):
    '''连接被关闭'''
    
class ServerStopped(Exception):
    '''服务已被关闭'''
