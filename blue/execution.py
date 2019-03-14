import logging
import time
import uuid
from typing import List, Dict

from dataclasses import asdict

from blue.blueprint import BlueprintManager
from blue.services import BlueprintExecutionStore, EventBus
from blue.datacontainers import BlueprintInstructionOutcome, BlueprintInstruction, Blueprint, Event, BlueprintExecution, BlueprintInstructionState

log = logging.getLogger(__name__)


class BlueprintExecutionManager:

    def __init__(self, event_bus: EventBus, blueprint_execution_store: BlueprintExecutionStore):
        self.event_bus = event_bus
        self.execution_store = blueprint_execution_store

    def start_execution(self, blueprint: Blueprint, boot_event: Event, execution_context: Dict):
        blueprint_execution_id = str(uuid.uuid4())
        boot_event.metadata['blueprint_execution_id'] = blueprint_execution_id

        instructions_states = []
        for instruction in blueprint.instructions:
            instruction_state = BlueprintInstructionState(instruction)
            instructions_states.append(instruction_state)

        blueprint_execution = BlueprintExecution(blueprint_execution_id, execution_context, blueprint, instructions_states)
        self.execution_store.store(blueprint_execution)
        self.event_bus.publish(boot_event)

    def get_all_executions(self):
        return self.execution_store.get_all()


class BlueprintExecutor:
    # DEFAULT_POLL_TIME = 10
    DEFAULT_WORKER_ID = "unnamed"

    def __init__(self, execution_manager: BlueprintExecutionManager, worker_id=None, max_iteration_count=None):
        self.blueprint_execution_store: BlueprintExecutionStore = execution_manager.execution_store
        self.event_bus: EventBus = execution_manager.event_bus

        self.worker_id = worker_id or self.DEFAULT_WORKER_ID
        self.iteration_count = 0
        self.max_iteration_count = max_iteration_count

    def run(self):
        log.info('Starting BlueprintExecutor')
        while True:
            self.iteration_count += 1
            blueprint_execution: BlueprintExecution = self.blueprint_execution_store.get_execution_to_process(self.worker_id)
            if not blueprint_execution:
                log.info("No BlueprintExecution found from blueprint_execution_store")
                continue

            instructions_to_process: List[BlueprintInstructionState] = [instr for instr in blueprint_execution.instructions_states if
                                                                        instr.status != 'PROCESSED']

            log.info(f"Processing BlueprintExecution {blueprint_execution.execution_id}. instructions_to_process={instructions_to_process}")
            for instruction in instructions_to_process:
                self._process_instruction(blueprint_execution, instruction)
            if self.max_iteration_count and self.iteration_count >= self.max_iteration_count:
                log.info(f"Completed Max iterations. Exiting.")
                break

    def _check_conditions(self, conditions):
        events = []
        for event_topic in conditions:
            event: Event = self.event_bus.get_event(event_topic)
            if not event:
                continue
            events.append(event)
        return events

    def _execute_outcome(self, outcome: BlueprintInstructionOutcome, execution_context: Dict, events: List[Event]):
        log.info(f"Found events {events}. Executing Outcome - Action {outcome.action} with Adapter {outcome.adapter} in context {execution_context}")
        adapter_result = outcome.adapter.adapt(events, execution_context)
        log.info(f"Adapter result - {adapter_result}")
        action_result = outcome.action.act(adapter_result)
        log.info(f"Action result - {action_result}")
        return action_result

    def _process_instruction(self, blueprint_execution: BlueprintExecution, instruction_state: BlueprintInstructionState):
        log.info(f"Processing BlueprintInstruction {instruction_state}")
        events = self._check_conditions(instruction_state.instruction.conditions)
        if len(events) != len(instruction_state.instruction.conditions):
            log.info(f"Could not find all necessary events to execute outcome. Found: {events} Required: {instruction_state.conditions}. Skipping.")
            return
        outcome_result = self._execute_outcome(instruction_state.instruction.outcome, blueprint_execution.execution_context, events)
        self.blueprint_execution_store.mark_instruction_complete(blueprint_execution.execution_id, instruction_state)
        return outcome_result
