# -*- coding: utf-8 -*-

class Singleton(type):
    def __init__(self, *args, **kwargs):
        self.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super().__call__(*args, **kwargs)
            return self.__instance
        else:
            if args or kwargs:
                self.__instance._setconfig(*args, **kwargs)
            return self.__instance
