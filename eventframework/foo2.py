import uuid
from typing import Dict, List

from dataclasses import dataclass

fixed_rate_order_blueprint_definition = {
    "name": "fixed_rate_order_blueprint_definition",
    "conditions": [
        {
            "event": "new_order",
            "action": "action_check_for_deposit",
            "adapter": "adapter_check_deposit_when_new_order"
        },
        {
            "event": "deposit_status",
            "action": "from_holding_branch",
            "adapter": "adapter_from_holding_when_deposit_status"
        },
        {
            "event": "deposit_status",
            "action": "to_holding_branch",
            "adapter": "adapter_to_holding_branch_when_deposit_status"
        }
    ]
}


class InvalidBlueprintException(Exception):
    pass


@dataclass
class Blueprint:
    name: str
    conditions: List[Dict]


@dataclass
class Event:
    metadata: Dict
    body: Dict


class BlueprintManager:
    blueprint_condition_component_names = ['event', 'action', 'adapter']

    def __init__(self, config):
        BlueprintManager.validate_config(config)
        self.config = config
        self.live_blueprints_by_name = {}

    @staticmethod
    def validate_config(config):
        if 'namespace' not in config:
            raise InvalidBlueprintException(f"Config must have 'namespace' key")
        for each in BlueprintManager.blueprint_condition_component_names:
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
            for condition_component in self.blueprint_condition_component_names:

                component_name = condition.get(condition_component)
                if not component_name:
                    raise InvalidBlueprintException(f"Component '{component_name}' must exist inside {i}th condition in blueprint_definition")

                component_object = self.config['namespace'][condition_component].get(component_name)
                if not component_object:
                    raise InvalidBlueprintException(f"As per configured namespace, no mapping found for {condition_component} {component_name}")

    def add_blueprint(self, blueprint_definition):
        self._validate_blueprint_definition(blueprint_definition)
        if blueprint_definition['name'] in self.live_blueprints_by_name:
            raise InvalidBlueprintException(f"Blueprint with name {blueprint_definition['name']} is already added")
        self.live_blueprints_by_name[blueprint_definition['name']] = Blueprint(blueprint_definition['name'], blueprint_definition['conditions'])


class BlueprintExecutionStore:
    def __init__(self, config):
        pass

    def store(self, blueprint: Blueprint, blueprint_execution_id: str):
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
        self.blueprint_execution_store = blueprint_execution_store

    def _select_blueprint(self, params: Dict) -> Blueprint:
        blueprint_name = "fixed_rate_order_blueprint"  # hardcoding for now
        return self.manager.live_blueprints_by_name[blueprint_name]

    def start_execution(self, boot_event: Event, blueprint_selector_params: Dict):
        blueprint = self._select_blueprint(blueprint_selector_params)

        blueprint_execution_id = uuid.uuid4()
        boot_event.metadata['blueprint_execution_id'] = blueprint_execution_id

        self.blueprint_execution_store.store(blueprint, blueprint_execution_id)
        self.event_bus.publish(boot_event)


if __name__ == "__main__":
    pass
