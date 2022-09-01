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
            if not args:
                return self.__instance
            if len(args) != 1 or kwargs:
                raise TypeError(
                    '__init__() takes from 1 to 2 ' +
                    'positional arguments but %d were given'
                    % (len(args) + len(kwargs))
                )
            self.__instance.default_timeout = args[0]
            return self.__instance
