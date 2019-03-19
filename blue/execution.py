import logging
import uuid
from typing import List, Dict

from dataclasses import asdict

from blue.base import BlueError, BlueprintInstructionExecutionStore, EventBus, Event, BlueprintInstructionOutcome, InstructionStatus, BlueprintInstructionState, \
    Blueprint, BlueprintExecution

from blue.base import Action, Adapter
from blue.blueprint import BlueprintManager

log = logging.getLogger(__name__)


class NoActionRequired(BlueError):
    pass


class BlueprintExecutionManager:

    def __init__(self, event_bus: EventBus, execution_store: BlueprintInstructionExecutionStore):
        self.event_bus = event_bus
        self.execution_store = execution_store

    def start_execution(self, blueprint: Blueprint, boot_event: Event, execution_context: Dict):
        blueprint_execution_id = str(uuid.uuid4())
        boot_event.metadata['blueprint_execution_id'] = blueprint_execution_id

        instructions_states = []
        for instruction in blueprint.instructions:
            instruction_state = BlueprintInstructionState(instruction, blueprint_execution_id)
            instructions_states.append(instruction_state)

        blueprint_execution = BlueprintExecution(blueprint_execution_id, execution_context, blueprint, instructions_states)
        self.execution_store.store(blueprint_execution)
        self.event_bus.publish(boot_event)


class BlueprintExecutor:
    # DEFAULT_POLL_TIME = 10
    DEFAULT_WORKER_ID = "unnamed"

    def __init__(self, execution_manager: BlueprintExecutionManager, blueprint_manager: BlueprintManager, worker_id=None, max_iteration_count=None):
        self.execution_store: BlueprintInstructionExecutionStore = execution_manager.execution_store
        self.event_bus: EventBus = execution_manager.event_bus
        self.blueprint_manager = blueprint_manager
        self.worker_id = worker_id or self.DEFAULT_WORKER_ID
        self.iteration_count = 0
        self.max_iteration_count = max_iteration_count

    def run(self):
        log.info('Starting BlueprintExecutor')
        while True:
            self.iteration_count += 1
            instruction_state: BlueprintInstructionState = self.execution_store.get_instruction_to_process(self.worker_id)

            if not instruction_state:
                log.info("No Blueprint Execution Instruction State found from execution_store")
                continue
            self._process_instruction(instruction_state)

            if self.max_iteration_count and self.iteration_count >= self.max_iteration_count:
                log.info(f"Completed Max iterations. Exiting.")
                break


    def _check_conditions(self, conditions, blueprint_execution_id):
        events = []
        for event_topic in conditions:
            event: Event = self.event_bus.get_event(event_topic, blueprint_execution_id)
            if not event:
                continue
            events.append(event)
        return events

    def _execute_outcome(self, outcome: BlueprintInstructionOutcome, blueprint_execution_id: str, events: List[Event]):
        execution_context = self.execution_store.get_execution_context_from_id(blueprint_execution_id)
        log.info(
            f"Found events {events}. Executing Outcome - Action {outcome.action} with Adapter {outcome.adapter} in context {execution_context}")

        adapter_instance: Adapter = outcome.adapter()
        adapter_result = adapter_instance.adapt(events, execution_context)
        log.info(f"Adapter result - {adapter_result}")

        action_instance: Action = outcome.action(self.event_bus)
        action_result = action_instance.act(adapter_result)
        log.info(f"Action result - {action_result}")
        return action_result

    def _process_instruction(self, instruction_state: BlueprintInstructionState):
        log.info(f"Processing BlueprintInstruction {instruction_state}")
        events = self._check_conditions(instruction_state.instruction.conditions, instruction_state.blueprint_execution_id)
        if len(events) != len(instruction_state.instruction.conditions):
            log.info(f"Could not find all necessary events to execute outcome. Found: {events} Required: {instruction_state.instruction.conditions}. Skipping.")
            self.execution_store.requeue(instruction_state)
            return
        try:
            self._execute_outcome(instruction_state.instruction.outcome, instruction_state.blueprint_execution_id, events)
        except NoActionRequired:
            log.info("Received NoActionRequired")
            self.execution_store.requeue(instruction_state)
        except Exception:
            log.exception("Unexpected Exception")
            self.execution_store.report_failure(instruction_state)
        else:
            self.execution_store.acknowledge_success(instruction_state)
