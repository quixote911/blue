import logging
import random
from abc import ABC, abstractmethod
from enum import auto
from typing import List

from base import BlueError
from blue.datacontainers import BlueprintExecution, Event, BlueprintInstructionState, InstructionStatus
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
    def get_instruction_to_process(self, worker_id) -> BlueprintInstructionState:
        pass

    @abstractmethod
    def set_status_for_instruction(self, instruction_state: BlueprintInstructionState, state: InstructionStatus):
        pass

    @abstractmethod
    def get_blueprint_from_id(self, blueprint_execution_id) -> BlueprintExecution:
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
        self._stored_blueprint_executions = {}
        self._stored_instruction_states = {}

    def _store_instruction_state(self, instruction_state: BlueprintInstructionState):
        self._stored_instruction_states[instruction_state.id_] = instruction_state

    def store(self, blueprint_execution: BlueprintExecution):
        self._stored_blueprint_executions[blueprint_execution.execution_id] = blueprint_execution
        for each in blueprint_execution.instructions_states:
            self._store_instruction_state(each)

    def get_instruction_to_process(self, worker_id) -> BlueprintExecution:
        instruction_id = random.choice(list(self._stored_instruction_states.keys()))
        instruction_state = self._stored_instruction_states[instruction_id]
        self.set_status_for_instruction(instruction_state, InstructionStatus.PROCESSING)
        return instruction_state

    def set_status_for_instruction(self, instruction_state: BlueprintInstructionState, state: InstructionStatus):
        instruction_state.status = state

    def get_blueprint_from_id(self, blueprint_execution_id) -> BlueprintExecution:
        return self._stored_blueprint_executions.get(blueprint_execution_id)


class InMemoryEventBus(EventBus):

    def __init__(self, config):
        super().__init__(config)
        self.event_by_topic = {}

    def publish(self, event):
        self.event_by_topic[event.topic] = event

    def get_event(self, topic):
        return self.event_by_topic.get(topic)
