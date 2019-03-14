import logging
from abc import ABC, abstractmethod
from enum import auto
from typing import List

from base import BlueError
from blue.datacontainers import BlueprintExecution, Event, BlueprintInstructionState
from util import AutoNameEnum

log = logging.getLogger(__name__)


class BlueprintExecutionStoreError(BlueError):
    pass


class BlueprintInstructionExecutionStore(ABC):
    def __init__(self, config):
        pass

    @abstractmethod
    def store(self, blueprint_execution: BlueprintExecution):
        pass

    @abstractmethod
    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        pass

    @abstractmethod
    def mark_instruction_complete(self, blueprint_execution_id: str, instruction_state: BlueprintInstructionState):
        pass

    @abstractmethod
    def get_all(self) -> List[BlueprintExecution]:
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


class InMemoryBlueprintInstructionExecutionStore(BlueprintInstructionExecutionStore):

    def __init__(self, config):
        super().__init__(config)
        self._stored_executions = {}
        self._in_flight_executions = []

    def store(self, blueprint_execution: BlueprintExecution):
        self._stored_executions[blueprint_execution.execution_id] = dict(blueprint_execution=blueprint_execution, in_flight=False)

    def get_all(self):
        return [x['blueprint_execution'] for x in self._stored_executions.values()]

    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        execution_to_process = self._stored_executions.pop()
        self._in_flight_executions.append(execution_to_process)
        return execution_to_process

    def mark_instruction_complete(self, blueprint_execution_id: str, instruction_state: BlueprintInstructionState):
        data = self._stored_executions.get(blueprint_execution_id)
        if not data:
            raise BlueprintExecutionStoreError(f"No such Blueprint found with blueprint_execution_id {blueprint_execution_id}")
        if not data['in_flight']:
            raise BlueprintExecutionStoreError(
                f"Inconcsistent state. Cannot mark instruction {instruction_state.id_} complete when BlueprintExecution {blueprint_execution_id} is not in flight")


        relevant_instruction_state = _get_relevant_instruction_states(data['blueprint_execution'].instructions_states)



class InMemoryEventBus(EventBus):

    def __init__(self, config):
        super().__init__(config)
        self.event_by_topic = {}

    def publish(self, event):
        self.event_by_topic[event.topic] = event

    def get_event(self, topic):
        return self.event_by_topic.get(topic)
