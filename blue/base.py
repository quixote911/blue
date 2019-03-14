from abc import ABC, abstractmethod


class BlueError(Exception):
    pass


class Action(ABC):
    def __init__(self, event_bus):
        self.event_bus = event_bus

    @abstractmethod
    def act(self, input):
        pass


class Adapter(ABC):
    @abstractmethod
    def adapt(self, context, events):
        pass
