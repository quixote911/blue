import logging
import random
from typing import Dict

from blue.base import BlueprintInstructionExecutionStore, EventBus, InstructionStatus, BlueprintInstructionState, BlueprintExecution

log = logging.getLogger(__name__)


class InMemoryBlueprintInstructionExecutionStore(BlueprintInstructionExecutionStore):

    def __init__(self, config):
        super().__init__(config)
        self._stored_blueprint_executions = {}
        self._stored_instruction_states = {}

    def _store_blueprint_execution(self, blueprint_execution: BlueprintExecution):
        self._stored_blueprint_executions[blueprint_execution.execution_id] = blueprint_execution

    def _store_instruction_state(self, instruction_state: BlueprintInstructionState):
        self._stored_instruction_states[instruction_state.id_] = instruction_state

    def get_instruction_to_process(self, worker_id) -> BlueprintExecution:
        instruction_id = random.choice(list(self._stored_instruction_states.keys()))
        instruction_state = self._stored_instruction_states[instruction_id]
        self.set_status_for_instruction(instruction_state, InstructionStatus.PROCESSING)
        return instruction_state

    def set_status_for_instruction(self, instruction_state: BlueprintInstructionState, state: InstructionStatus):
        instruction_state.status = state

    def get_execution_context_from_id(self, blueprint_execution_id) -> Dict:
        return self._stored_blueprint_executions.get(blueprint_execution_id).execution_context


class InMemoryEventBus(EventBus):

    def __init__(self, config):
        super().__init__(config)
        self.event_by_topic = {}

    def publish(self, event):
        self.event_by_topic[event.topic] = event

    def get_event(self, topic):
        return self.event_by_topic.get(topic)
