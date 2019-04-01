from concurrent import futures

import pytest
from moto import mock_s3, mock_sqs

from blue.base import InstructionStatus, Event, BlueprintExecution, BlueprintInstructionExecutionStore
from blue.impl.persistent import PersistentBlueprintInstructionExecutionStore, PersistentEventBus
from blue.util import superjson
from conftest import get_random_string


def setup_module(module):
    """ setup any state specific to the execution of the given module."""


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
    method.
    """


@pytest.fixture()
def instruction_execution_store(sample_blueprint_manager, sample_execution_store_config):
    with mock_sqs():
        store = PersistentBlueprintInstructionExecutionStore(sample_blueprint_manager, sample_execution_store_config)
        yield store
        store.remove_effects()


def test_superjson(sample_blueprint_execution):
    exec = superjson(sample_blueprint_execution)
    assert isinstance(exec, str)


def test_db_store(instruction_execution_store, sample_blueprint_execution):
    sample_blueprint_execution.execution_id += get_random_string(5)
    store = instruction_execution_store
    store.store(sample_blueprint_execution)
    context = store.get_execution_context_from_id(sample_blueprint_execution.execution_id)
    assert context == sample_blueprint_execution.execution_context


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


def get_number_of_messages_in_queue(store: PersistentBlueprintInstructionExecutionStore):
    response = store.sqs.get_queue_attributes(
        QueueUrl=store._queue_url,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])


def test_locking(instruction_execution_store, sample_blueprint_execution):
    sample_blueprint_execution.execution_id += get_random_string(5)
    store = instruction_execution_store
    assert get_number_of_messages_in_queue(store) == 0
    store.store(sample_blueprint_execution)
    assert get_number_of_messages_in_queue(store) == 1
    ex = futures.ThreadPoolExecutor(max_workers=2)
    results = ex.map(store.get_instruction_to_process, range(5))
    eval_results = list(results)
    assert len([x for x in eval_results if x]) == 1


def test_locking_with_mistake(instruction_execution_store, sample_blueprint_execution):
    sample_blueprint_execution.execution_id += get_random_string(5)
    store = instruction_execution_store
    assert get_number_of_messages_in_queue(store) == 0
    store.store(sample_blueprint_execution)
    assert get_number_of_messages_in_queue(store) == 1
    store.sqs.send_message(
        QueueUrl=store._queue_url,
        MessageBody=superjson(sample_blueprint_execution.instructions_states[0])
    )
    assert get_number_of_messages_in_queue(store) == 2

    ex = futures.ThreadPoolExecutor(max_workers=5)
    results = ex.map(store.get_instruction_to_process, range(5))
    eval_results = list(results)
    assert len([x for x in eval_results if x]) == 1
