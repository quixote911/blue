import logging

from blue.base import Action, Adapter, Event
from blue.blueprint import BlueprintManager
from blue.execution import BlueprintExecutionManager, BlueprintExecutor
from blue.impl.inmemory import InMemoryEventBus, InMemoryBlueprintInstructionExecutionStore
from fixtures import *

log = logging.getLogger(__name__)

initialize_fixtures()


def basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition):
    event_bus = InMemoryEventBus(dict())
    execution_store = InMemoryBlueprintInstructionExecutionStore(dict())
    bem = BlueprintExecutionManager(event_bus, execution_store)

    bm = BlueprintManager(sample_namespace_config)
    bm.add_blueprint(sample_blueprint_definition)
    blueprint_to_execute = bm.live_blueprints_by_name['test_blueprint_1']

    boot_event = Event('new_order')
    execution_context = {'order_id': 'ABCASD123123', 'is_express': False, 'is_fixed_rate': True}
    bem.start_execution(blueprint_to_execute, boot_event, execution_context)
    return bem


def test_blueprint_manager_initalize(sample_namespace_config):
    bm = BlueprintManager(sample_namespace_config)
    assert bm is not None


def test_add_blueprint(sample_namespace_config, sample_blueprint_definition):
    bm = BlueprintManager(sample_namespace_config)
    bm.add_blueprint(sample_blueprint_definition)
    assert sample_blueprint_definition['name'] in bm.live_blueprints_by_name


def test_blueprint_execution_manager_start_execution(sample_namespace_config, sample_blueprint_definition):
    bem = basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition)
    # assert len(bem.execution_store.get_all()) == 1
    # assert bem.get_all_executions()[0].blueprint.name == 'test_blueprint_1'


def test_blueprint_executor(sample_namespace_config, sample_blueprint_definition):
    bem = basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition)
    bex = BlueprintExecutor(bem, 'worker-testrunner', 1)
    bex.run()
