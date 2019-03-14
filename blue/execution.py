import logging
import time
import uuid
from typing import List, Dict

from blue.blueprint import BlueprintManager
from blue.services import BlueprintExecutionStore, EventBus
from blue.datacontainers import BlueprintInstructionOutcome, BlueprintInstruction, Blueprint, Event, BlueprintExecution

log = logging.getLogger(__name__)


class BlueprintExecutor:
    DEFAULT_POLL_TIME = 10

    def __init__(self, blueprint_execution_store: BlueprintExecutionStore, event_bus: EventBus):
        self.blueprint_execution_store = blueprint_execution_store
        self.event_bus = event_bus

        self.worker_id = self._get_worker_id()
        self.iteration_count = 0

    def _get_worker_id(self):
        return "dummy_worker_id"

    def _sleep(self):
        if self.iteration_count != 0:
            log.info(f"Sleeping for {self.DEFAULT_POLL_TIME} seconds")
            time.sleep(self.DEFAULT_POLL_TIME)
        self.iteration_count += 1

    def run(self):
        log.info('Starting BlueprintExecutor')
        while True:  # TODO: Maybe parallel
            self._sleep()

            blueprint_execution: BlueprintExecution = self.blueprint_execution_store.get_execution_to_process(self.worker_id)
            if not blueprint_execution:
                log.info("No BlueprintExecution found from blueprint_execution_store")
                continue

            instructions_to_process = [instr for instr in blueprint_execution.blueprint.instructions if instr.status != 'PROCESSED']
            log.info(f"Processing BlueprintExecution {blueprint_execution.execution_id}. instructions_to_process={instructions_to_process}")
            for instruction in instructions_to_process:  # TODO: parallel
                self._process_instruction(blueprint_execution, instruction)

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


class BlueprintExecutionManager:

    def __init__(self, blueprint_manager: BlueprintManager, event_bus: EventBus, blueprint_execution_store: BlueprintExecutionStore):
        self.manager = blueprint_manager
        self.event_bus = event_bus
        self.execution_store = blueprint_execution_store

    def _select_blueprint(self, execution_context: Dict) -> Blueprint:
        blueprint_name = "fixed_rate_order_blueprint"  # hardcoding for now
        return self.manager.live_blueprints_by_name[blueprint_name]

    def start_execution(self, boot_event: Event, execution_context: Dict):
        blueprint = self._select_blueprint(execution_context)

        blueprint_execution_id = uuid.uuid4()
        boot_event.metadata['blueprint_execution_id'] = blueprint_execution_id

        blueprint_execution = BlueprintExecution(blueprint_execution_id, execution_context, blueprint)
        self.execution_store.store(blueprint_execution)
        self.event_bus.publish(boot_event)
