import logging
import uuid
from typing import Dict, List, Optional, Union

import time
from dataclasses import dataclass

log = logging.getLogger(__name__)

fixed_rate_order_blueprint_definition = {
    "name": "fixed_rate_order_blueprint_definition",
    "conditions": [
        {
            "events": ["new_order"],
            "action": "action_check_for_deposit",
            "adapter": "adapter_check_deposit_when_new_order"
        },
        {
            "events": ["deposit_status"],
            "action": "from_holding_branch",
            "adapter": "adapter_from_holding_when_deposit_status"
        },
        {
            "events": ["deposit_status"],
            "action": "to_holding_branch",
            "adapter": "adapter_to_holding_branch_when_deposit_status"
        }
    ]
}


class InvalidBlueprintException(Exception):
    pass


class Event:
    pass


class Action:
    pass


class Adapter:
    pass


@dataclass
class BlueprintCondition:
    events: List[Event]
    action: Action
    adapter: Adapter


@dataclass
class Blueprint:
    name: str
    conditions: List[BlueprintCondition]


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
    blueprint_condition_attribute_names = dict(BlueprintCondition).keys()

    def __init__(self, config):
        BlueprintManager.validate_config(config)
        self.config = config
        self.live_blueprints_by_name = {}

    @staticmethod
    def validate_config(config):
        if 'namespace' not in config:
            raise InvalidBlueprintException(f"Config must have 'namespace' key")
        for each in BlueprintManager.blueprint_condition_attribute_names:
            if each not in config['namespace']:
                raise InvalidBlueprintException(f"Namespace must have key '{each}'")

    def _validate_blueprint_definition(self, blueprint_definition):
        if not blueprint_definition:
            raise InvalidBlueprintException(f"Blueprint definition seems to be empty")

        if 'name' not in blueprint_definition:
            raise InvalidBlueprintException(f"Blueprint definition must have key 'name'")

        blueprint_conditions = blueprint_definition.get('conditions')
        if not blueprint_conditions:
            raise InvalidBlueprintException("Blueprint definition must have key 'conditions'")

        for i, condition in enumerate(blueprint_conditions):
            for condition_attribute_name in self.blueprint_condition_attribute_names:

                component_name = condition.get(condition_attribute_name)
                if not component_name:
                    raise InvalidBlueprintException(f"Component '{component_name}' must exist inside {i}th condition in blueprint_definition")

                component_object = self.config['namespace'][condition_attribute_name].get(component_name)
                if not component_object:
                    raise InvalidBlueprintException(f"As per configured namespace, no mapping found for {condition_attribute_name} {component_name}")

    def _objectify_condition(self, condition_definition: Dict) -> BlueprintCondition:
        def _objectify(attribute, component_name):
            return self.config['namespace'][attribute][component_name]

        mydict = {}
        for attribute, component_name in condition_definition:
            mydict[attribute] = _objectify(attribute, component_name)

        return BlueprintCondition(**mydict)

    def _convert_definitions_to_concrete(self, blueprint_definition) -> Blueprint:
        conditions = []
        for definition in blueprint_definition['conditions']:
            condition: BlueprintCondition = self._objectify_condition(definition)
            conditions.append(condition)
        return Blueprint(blueprint_definition['name'], conditions)

    def add_blueprint(self, blueprint_definition):
        self._validate_blueprint_definition(blueprint_definition)
        blueprint = self._convert_definitions_to_concrete(blueprint_definition)
        if blueprint.name in self.live_blueprints_by_name:
            raise InvalidBlueprintException(f"Blueprint with name {blueprint_definition['name']} is already added")
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

    def __init__(self, blueprint_manager: BlueprintManager, blueprint_execution_store: BlueprintExecutionStore, event_bus: EventBus):
        self.blueprint_execution_store = blueprint_execution_store
        self.event_bus = event_bus
        self.blueprint_manager = blueprint_manager

        self.worker_id = self._get_worker_id()

    def _get_worker_id(self):
        return "dummy_worker_id"

    def run(self):
        log.info('Starting BlueprintExecutor')
        iteration_count = 0
        while True:  # TODO: Maybe parallel
            if iteration_count != 0:
                log.info(f"Sleeping for {self.DEFAULT_POLL_TIME} seconds")
                time.sleep(self.DEFAULT_POLL_TIME)
            iteration_count += 1

            blueprint_execution: BlueprintExecution = self.blueprint_execution_store.get_execution_to_process(self.worker_id)
            if not blueprint_execution:
                log.info("No BlueprintExecution found from blueprint_execution_store")
                continue
            log.info(f"Processing BlueprintExecution {blueprint_execution.execution_id}")
            for condition in blueprint_execution.blueprint.conditions:  # TODO: parallel
                log.info(f"Processing BlueprintCondition {condition}")
                events = self.event_bus.consume_latest_events(blueprint_execution.execution_id, events)
                if not events:
                    log.info(f"Could not find events {events} in event_bus")
                    continue

                self._execute_condition(blueprint_execution, condition, events)

    def _execute_condition(self, blueprint_execution: BlueprintExecution, condition: BlueprintCondition, events: List[Event]):
        log.info(f"Found events {events}. Executing Condition - Action {condition.action} with Adapter {condition.adapter} in context {blueprint_execution.execution_context}")
        adapter_result = condition.adapter.adapt(events, blueprint_execution.execution_context)
        log.info(f"Adapter result - {adapter_result}")
        action_result = condition.action.act(adapter_result)
        log.info(f"Action result - {action_result}")




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



# TODO: Event - what does it mean to have event definition and event class. Should i model it as event_name in the condition