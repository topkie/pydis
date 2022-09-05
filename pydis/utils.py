# -*- coding: utf-8 -*-

from threading import Lock

class Singleton(type):
    def __init__(self, *args, **kwargs):
        self.__instance = None
        self.__mutex=Lock()
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            with self.__mutex:
                if self.__instance is None:
                    self.__instance = super().__call__(*args, **kwargs)
        return self.__instance
