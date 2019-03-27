import logging

from blue.base import Action, Adapter, Event
from blue.blueprint import BlueprintManager
from blue.execution import BlueprintExecutionManager, BlueprintExecutor
from blue.impl.inmemory import InMemoryEventBus, InMemoryBlueprintInstructionExecutionStore
# from conftest import initialize_fixtures

log = logging.getLogger(__name__)

# initialize_fixtures()


def basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition):
    bm = BlueprintManager(sample_namespace_config)
    bm.add_blueprint(sample_blueprint_definition)
    blueprint_to_execute = bm.live_blueprints_by_name['test_blueprint_1']

    event_bus = InMemoryEventBus(dict())
    execution_store = InMemoryBlueprintInstructionExecutionStore(bm, dict())
    bem = BlueprintExecutionManager(event_bus, execution_store)

    boot_event = Event('new_order')
    execution_context = {'order_id': 'ABCASD123123', 'is_express': False, 'is_fixed_rate': True}
    bem.start_execution(blueprint_to_execute, boot_event, execution_context)
    return bem, bm
#
# #
# def test_blueprint_manager_initalize(sample_namespace_config):
#     bm = BlueprintManager(sample_namespace_config)
#     assert bm is not None
#
#
# def test_add_blueprint(sample_namespace_config, sample_blueprint_definition):
#     bm = BlueprintManager(sample_namespace_config)
#     bm.add_blueprint(sample_blueprint_definition)
#     assert sample_blueprint_definition['name'] in bm.live_blueprints_by_name
#
#
# def test_blueprint_execution_manager_start_execution(sample_namespace_config, sample_blueprint_definition):
#     bem, _ = basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition)
#     # assert len(bem.execution_store.get_all()) == 1
#     # assert bem.get_all_executions()[0].blueprint.name == 'test_blueprint_1'
#
#
# def test_blueprint_executor(sample_namespace_config, sample_blueprint_definition):
#     bem, bm = basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition)
#     bex = BlueprintExecutor(bem, bm, 'worker-testrunner', 1)
#     bex.run()

def test_blueprint_executor_timeout(sample_namespace_config, sample_blueprint_definition):
    bem, bm = basic_initialize_execution_manager(sample_namespace_config, sample_blueprint_definition)
    bex = BlueprintExecutor(bem, bm, 'worker-testrunner', 3, True)

    ids_in_exec_store = [ id_ for id_ in bem.execution_store._stored_blueprint_executions]
    bem.event_bus.publish(Event('deposit_timeout', metadata=dict(blueprint_execution_id=ids_in_exec_store[0])))
    bex.run()
    instruction = bem.execution_store.get_instruction_to_process()
    assert instruction == None



