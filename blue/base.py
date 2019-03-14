from abc import ABC, abstractmethod

class BlueError(Exception):
    pass

class Event():
    pass


class Action(ABC):
    @abstractmethod
    def act(self, input):
        pass


class Adapter(ABC):
    @abstractmethod
    def adapt(self, context, events):
        pass