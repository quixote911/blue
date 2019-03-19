import pytest
from moto import mock_s3, mock_sqs

from blue.base import InstructionStatus, Event
from blue.impl.persistent import PersistentBlueprintInstructionExecutionStore, PersistentEventBus
from blue.util import superjson
from conftest import get_random_string


def setup_module(module):
    """ setup any state specific to the execution of the given module."""


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
    method.
    """


@pytest.fixture(scope="module")
def instruction_execution_store(sample_blueprint_manager, sample_execution_store_config):
    with mock_sqs():
        store = PersistentBlueprintInstructionExecutionStore(sample_blueprint_manager, sample_execution_store_config)
        # store.remove_effects()
        # store.rerun_migrations()
        yield store
        store.remove_effects()


def test_superjson(sample_blueprint_execution):
    exec = superjson(sample_blueprint_execution)
    assert isinstance(exec, str)


#
def test_db_store(instruction_execution_store, sample_blueprint_execution):
    sample_blueprint_execution.execution_id += get_random_string(5)
    store = instruction_execution_store
    store.store(sample_blueprint_execution)
    context = store.get_execution_context_from_id(sample_blueprint_execution.execution_id)
    assert context == sample_blueprint_execution.execution_context

#
def test_get_instruction(instruction_execution_store, sample_blueprint_execution):
    sample_blueprint_execution.execution_id += get_random_string(5)
    store = instruction_execution_store
    store.store(sample_blueprint_execution)
    instruction_state = store.get_instruction_to_process('testcase_worker_id')
    assert instruction_state.status == InstructionStatus.PROCESSING


def test_event_bus(sample_execution_store_config, sample_event):
    eventbus = PersistentEventBus(sample_execution_store_config)
    eventbus.publish(sample_event)
    event: Event = eventbus.get_event(sample_event.topic, sample_event.metadata['blueprint_execution_id'])
    assert sample_event.metadata['blueprint_execution_id'] == event.metadata['blueprint_execution_id']
