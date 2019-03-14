from abc import ABC, abstractmethod

from blue.datacontainers import BlueprintExecution, Event


class BlueprintExecutionStore(ABC):
    def __init__(self, config):
        pass

    @abstractmethod
    def store(self, blueprint_execution: BlueprintExecution):
        pass

    @abstractmethod
    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        pass

    @abstractmethod
    def get_all(self):
        pass

class EventBus:
    def __init__(self, config):
        pass

    @abstractmethod
    def publish(self, event: Event):
        pass



class InMemoryBlueprintExecutionStore(BlueprintExecutionStore):
    def __init__(self, config):
        super().__init__(config)
        self._stored_executions = []

    def store(self, blueprint_execution: BlueprintExecution):
        self._stored_executions.append(blueprint_execution)

    def get_all(self):
        return self._stored_executions

    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        pass


class InMemoryEventBus(EventBus):

    def __init__(self, config):
        super().__init__(config)
        self.events_by_name = {}

    def publish(self, event):
        self.events_by_name[event.name] = event