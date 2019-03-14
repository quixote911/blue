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

    @abstractmethod
    def get_event(self, topic):
        pass

class InMemoryBlueprintExecutionStore(BlueprintExecutionStore):
    def __init__(self, config):
        super().__init__(config)
        self._stored_executions = []
        self._in_flight_executions = []

    def store(self, blueprint_execution: BlueprintExecution):
        self._stored_executions.append(blueprint_execution)

    def get_all(self):
        return self._stored_executions

    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        execution_to_process = self._stored_executions.pop()
        self._in_flight_executions.append(execution_to_process)
        return execution_to_process


class InMemoryEventBus(EventBus):


    def __init__(self, config):
        super().__init__(config)
        self.event_by_topic = {}

    def publish(self, event):
        self.event_by_topic[event.topic] = event

    def get_event(self, topic):
        return self.event_by_topic.get(topic)
