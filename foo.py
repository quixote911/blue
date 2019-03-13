import logging

from blue.base import Event
from blue.blueprint import BlueprintManager
from blue.execution import BlueprintExecutionManager
from blue.services import BlueprintExecutionStore, EventBus

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