import logging
import uuid
from typing import Dict, List, Optional, Union

import time
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

fixed_rate_order_blueprint_definition = {
    "name": "fixed_rate_order_blueprint_definition",
    "instructions": [
        {
            "conditions": ["new_order"],
            "outcome": {
                "action": "action_check_for_deposit",
                "adapter": "adapter_check_deposit_when_new_order"
            }
        },
        {
            "conditions": ["deposit_status"],
            "outcome": {
                "action": "from_holding_branch",
                "adapter": "adapter_from_holding_when_deposit_status"
            }
        },
        {
            "conditions": ["deposit_status"],
            "outcome": {
                "action": "to_holding_branch",
                "adapter": "adapter_to_holding_branch_when_deposit_status"
            }
        }
    ]
}


class InvalidBlueprintDefinition(Exception):
    pass


class Event:
    pass


class Action:
    pass


class Adapter:
    pass


@dataclass
class BlueprintInstructionOutcome:
    action: Action
    adapter: Adapter


def generate_random_id():
    return str(uuid.uuid4())


@dataclass
class BlueprintInstruction:
    id_: str = field(default_factory=generate_random_id)
    conditions: List[str]
    outcome: BlueprintInstructionOutcome


@dataclass
class Blueprint:
    name: str
    instructions: List[BlueprintInstruction]


@dataclass
class Event:
    metadata: Dict
    body: Dict


@dataclass
class BlueprintExecution:
    execution_id: str
    execution_context: Dict
    blueprint: Blueprint


class BlueprintManager:
    instruction_outcome_attribute_names = dict(BlueprintInstructionOutcome).keys()

    def __init__(self, config):
        BlueprintManager.validate_config(config)
        self.config = config
        self.live_blueprints_by_name = {}

    @staticmethod
    def validate_config(config):
        if 'namespace' not in config:
            raise InvalidBlueprintDefinition(f"Config must have 'namespace' key")
        for each in BlueprintManager.instruction_outcome_attribute_names:
            if each not in config['namespace']:
                raise InvalidBlueprintDefinition(f"Namespace must have key '{each}'")

    def _validate_blueprint_definition(self, blueprint_definition):
        if not blueprint_definition:
            raise InvalidBlueprintDefinition(f"Blueprint definition seems to be empty")

        if 'name' not in blueprint_definition:
            raise InvalidBlueprintDefinition(f"Blueprint definition must have key 'name'")

        blueprint_instructions = blueprint_definition.get('instructions')
        if not blueprint_instructions:
            raise InvalidBlueprintDefinition("Blueprint definition must have key 'instructions'")

        for i, instruction in enumerate(blueprint_instructions):
            if 'conditions' not in instruction or 'outcome' not in instruction:
                raise InvalidBlueprintDefinition(f"Instruction must have keys 'conditions' and 'outcome': {instruction}")
            for outcome_attribute_name in self.instruction_outcome_attribute_names:
                component_name_by_attribute_name = instruction['outcome']
                component_name = component_name_by_attribute_name.get(outcome_attribute_name)
                if not component_name:
                    raise InvalidBlueprintDefinition(
                        f"Instruction Outcome attribute '{outcome_attribute_name}' must exist inside instruction 'outcome' of {instruction}")

                component_object = self.config['namespace'][outcome_attribute_name].get(component_name)
                if not component_object:
                    raise InvalidBlueprintDefinition(
                        f"As per configured namespace {self.config}, no component is defined for attribute_name={outcome_attribute_name} componenet_name={component_name}")

    def _objectify_instruction(self, instruction_definition: Dict) -> BlueprintInstruction:
        def _objectify(attribute, component_name):
            return self.config['namespace'][attribute][component_name]

        outcome = BlueprintInstructionOutcome(
            action=_objectify('action', instruction_definition['outcome']['action']),
            adapter=_objectify('adapter', instruction_definition['outcome']['adapter'])
        )

        return BlueprintInstruction(conditions=instruction_definition['conditions'], outcome=outcome)

    def _convert_blueprint_definition_to_object(self, blueprint_definition) -> Blueprint:
        instructions = []
        for definition in blueprint_definition['instructions']:
            instruction: BlueprintInstruction = self._objectify_instruction(definition)
            instructions.append(instruction)
        return Blueprint(blueprint_definition['name'], instructions)

    def add_blueprint(self, blueprint_definition):
        self._validate_blueprint_definition(blueprint_definition)
        blueprint = self._convert_blueprint_definition_to_object(blueprint_definition)
        if blueprint.name in self.live_blueprints_by_name:
            raise InvalidBlueprintDefinition(f"Blueprint with name {blueprint_definition['name']} is already added")
        self.live_blueprints_by_name[blueprint.name] = blueprint


class BlueprintExecutionStore:
    def __init__(self, config):
        pass

    def store(self, blueprint_execution: BlueprintExecution):
        pass

    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        pass


class EventBus:
    def __init__(self, config):
        pass

    def publish(self, event):
        pass


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

            instructions_to_process = [c for c in blueprint_execution.blueprint.instructions if c.status != 'PROCESSED']
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


def add_new_order_for_execution():
    blueprint_manager_config = []
    blueprint_manager = BlueprintManager(blueprint_manager_config)
    blueprint_manager.add_blueprint(fixed_rate_order_blueprint_definition)

    event_bus = EventBus()
    execution_store = BlueprintExecutionStore()
    execution_manager = BlueprintExecutionManager(blueprint_manager, event_bus, execution_store)

    new_order_event = Event()
    execution_manager.start_execution(new_order_event, dict())


def execute():
    pass