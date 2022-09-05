# -*- coding: utf-8 -*-

from enum import Enum


class message(Enum):
    GET = 1
    SET = 2
    CALL = 3
    RETURN = 10
    ERROR = 11
