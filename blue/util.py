import uuid
from enum import Enum


def generate_random_id():
    return str(uuid.uuid4())

class AutoNameEnum(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    @classmethod
    def has(cls, value):
        return any(value == item.value for item in cls)
