import functools
import inspect
import json
import random
import string
import uuid
from enum import Enum
from json import JSONEncoder

from dataclasses import is_dataclass, asdict



def generate_random_id():
    return str(uuid.uuid4())


def superjson(obj) -> str:
    return json.dumps(obj, default=_serialize_all)


def _serialize_all(obj):
    if is_dataclass(obj):
        return asdict(obj)
    if inspect.isclass(obj):
        return obj.__name__
    if isinstance(obj, Enum):
        return obj.value
    else:
        return str(obj)


class AutoNameEnum(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    @classmethod
    def has(cls, value):
        return any(value == item.value for item in cls)


class BlueEncoder(JSONEncoder):
    def default(self, obj):
        if inspect.isclass(obj):
            return obj.__name__
        return json.JSONEncoder.default(self, obj)


# >>> class ComplexEncoder(json.JSONEncoder):
#     ...     def default(self, obj):
#     ...         if isinstance(obj, complex):
#     ...             return [obj.real, obj.imag]
# ...         # Let the base class default method raise the TypeError
# ...         return json.JSONEncoder.default(self, obj)


blue_json_dumps = functools.partial(json.dumps, cls=BlueEncoder)
