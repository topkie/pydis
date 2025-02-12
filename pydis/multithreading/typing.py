
from typing import Any, Tuple, Union

from .message import message

RequestT = Tuple[message, str, Union[Any, None]]
ResponseT = Tuple[message, Union[Any, None]]
MessageT = Union[RequestT, ResponseT]
