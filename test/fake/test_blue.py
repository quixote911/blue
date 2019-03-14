import logging

from blue.base import Event, Action, Adapter
from blue.blueprint import BlueprintManager
from blue.execution import BlueprintExecutionManager
from blue.services import BlueprintExecutionStore, EventBus

log = logging.getLogger(__name__)


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
            'check_deposit': CheckForDeposit(),
            'transfer_to_exchange': TransferToExchange()
        },
        'adapter': {
            'basic_adapter': BasicAdapter()
        }
    }
}

test_blueprint_definition = {
    "name": "test_blueprint_1",
    "instructions": [
        {
            "conditions": ["new_order"],
            "outcome": {
                "action": "check_deposit",
                "adapter": "basic_adapter"
            }
        },
        {
            "conditions": ["deposit_status"],
            "outcome": {
                "action": "transfer_to_exchange",
                "adapter": "basic_adapter"
            }
        }
    ]
}


def test_blueprint_manager_initalize():
    bm = BlueprintManager(blueprint_manager_config)
    assert bm is not None


def test_add_blueprint():
    bm = BlueprintManager(blueprint_manager_config)
    bm.add_blueprint(test_blueprint_definition)
    assert test_blueprint_definition['name'] in bm.live_blueprints_by_name
