from moto import mock_s3, mock_sqs

from blue.base import InstructionStatus
from blue.impl.db import DbBlueprintInstructionExecutionStore
from blue.util import superjson
from fixtures import *



def setup_module(module):
    """ setup any state specific to the execution of the given module."""

def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
    method.
    """

@pytest.fixture(scope="module")
def instruction_execution_store(sample_execution_store_config):
    with mock_sqs():
        store = DbBlueprintInstructionExecutionStore(sample_execution_store_config)
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

#
def test_get_instruction(instruction_execution_store, sample_blueprint_execution):
    sample_blueprint_execution.execution_id += get_random_string(5)
    store = instruction_execution_store
    store.store(sample_blueprint_execution)
    instruction_state = store.get_instruction_to_process('testcase_worker_id')
    assert instruction_state.status == InstructionStatus.PROCESSING
