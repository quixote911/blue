import logging

from blue.base import Event, Action, Adapter
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



class CheckForDeposit(Action):
    def act(self, input):
        print("Action - CheckedForDeposit")

class TransferToExchange(Action):
    def act(self, input):
        print("Action - CheckedForDeposit")

class BasicAdapter(Adapter):
    def adapt(self, context, events):
        return dict(foo='bar')


blueprint_manager_config = {
    'namespace': {
        'action': {
            'check_deposit':  CheckForDeposit(),
            'transfer_to_exchange': TransferToExchange()
        },
        'adapter': {
            'basic_adapter': BasicAdapter()
        }
    }
}

def test_blueprint_manager_initalize():
    blueprint_manager = BlueprintManager(blueprint_manager_config)

def test_foo():
    pass