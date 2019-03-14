import logging

from blue.base import Action, Adapter
from blue.blueprint import BlueprintManager
from blue.execution import BlueprintExecutionManager, BlueprintExecutor
from blue.services import BlueprintExecutionStore, EventBus, InMemoryEventBus, InMemoryBlueprintExecutionStore
from datacontainers import Event

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


def basic_initialize_execution_manager():
    event_bus = InMemoryEventBus(dict())
    execution_store = InMemoryBlueprintExecutionStore(dict())
    bem = BlueprintExecutionManager(event_bus, execution_store)

    bm = BlueprintManager(blueprint_manager_config)
    bm.add_blueprint(test_blueprint_definition)
    blueprint_to_execute = bm.live_blueprints_by_name['test_blueprint_1']

    boot_event = Event('new_order', )
    execution_context = {'order_id': 'ABCASD123123', 'is_express': False, 'is_fixed_rate': True}
    bem.start_execution(blueprint_to_execute, boot_event, execution_context)
    return bem

def test_blueprint_manager_initalize():
    bm = BlueprintManager(blueprint_manager_config)
    assert bm is not None


def test_add_blueprint():
    bm = BlueprintManager(blueprint_manager_config)
    bm.add_blueprint(test_blueprint_definition)
    assert test_blueprint_definition['name'] in bm.live_blueprints_by_name



def test_blueprint_execution_manager_start_execution():
    bem = basic_initialize_execution_manager()
    assert len(bem.execution_store.get_all()) == 1
    assert bem.get_all_executions()[0].blueprint.name == 'test_blueprint_1'


def test_blueprint_executor():
    bem = basic_initialize_execution_manager()
    bex = BlueprintExecutor(bem)




