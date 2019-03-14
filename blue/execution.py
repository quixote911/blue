import logging
import time
import uuid
from typing import List, Dict

from blue.blueprint import BlueprintManager
from blue.services import BlueprintExecutionStore, EventBus
from blue.datacontainers import BlueprintInstructionOutcome, BlueprintInstruction, Blueprint, Event, BlueprintExecution

log = logging.getLogger(__name__)


class BlueprintExecutionManager:

    def __init__(self, event_bus: EventBus, blueprint_execution_store: BlueprintExecutionStore):
        self.event_bus = event_bus
        self.execution_store = blueprint_execution_store

    def start_execution(self, blueprint: Blueprint, boot_event: Event, execution_context: Dict):
        blueprint_execution_id = str(uuid.uuid4())
        boot_event.metadata['blueprint_execution_id'] = blueprint_execution_id

        blueprint_execution = BlueprintExecution(blueprint_execution_id, execution_context, blueprint)
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

    def _get_worker_id(self):
        return "dummy_worker_id"

    def run(self):
        log.info('Starting BlueprintExecutor')
        while True:  # TODO: Maybe parallel
            self.iteration_count += 1
            blueprint_execution: BlueprintExecution = self.blueprint_execution_store.get_execution_to_process(self.worker_id)
            if not blueprint_execution:
                log.info("No BlueprintExecution found from blueprint_execution_store")
                continue

            instructions_to_process = [instr for instr in blueprint_execution.blueprint.instructions if instr.status != 'PROCESSED']
            log.info(f"Processing BlueprintExecution {blueprint_execution.execution_id}. instructions_to_process={instructions_to_process}")
            for instruction in instructions_to_process:  # TODO: parallel
                self._process_instruction(blueprint_execution, instruction)
            if self.max_iteration_count and self.iteration_count >= self.max_iteration_count:
                log.info(f"Completed Max iterations. Exiting.")
                break

    def _process_instruction(self, blueprint_execution: BlueprintExecution, instruction: BlueprintInstruction):
        log.info(f"Processing BlueprintInstruction {instruction}")
        events: List[Event] = self.event_bus.consume_latest_events(blueprint_execution.execution_id, instruction.conditions)
        if not events:
            log.info(f"Could not find necessary events {instruction.conditions} in event_bus to execute outcome of this instruction")
            return
        outcome_result = self._execute_outcome(instruction.outcome, blueprint_execution.execution_context, events)
        self.blueprint_execution_store.mark_instruction_complete(blueprint_execution, instruction, outcome_result)
        return outcome_result

    def _execute_outcome(self, outcome: BlueprintInstructionOutcome, execution_context: Dict, events: List[Event]):
        log.info(f"Found events {events}. Executing Outcome - Action {outcome.action} with Adapter {outcome.adapter} in context {execution_context}")
        adapter_result = outcome.adapter.adapt(events, execution_context)
        log.info(f"Adapter result - {adapter_result}")
        action_result = outcome.action.act(adapter_result)
        log.info(f"Action result - {action_result}")
        return action_result
