from blue.base import InstructionStatus
from blue.impl.db import DbBlueprintInstructionExecutionStore
from blue.util import superjson
from fixtures import *


def test_superjson(sample_blueprint_execution):
    exec = superjson(sample_blueprint_execution)
    assert isinstance(exec, str)


def test_db_store(sample_execution_store_config, sample_blueprint_execution):
    store = DbBlueprintInstructionExecutionStore(sample_execution_store_config)
    store.remove_effects()
    store.rerun_migrations()
    store.store(sample_blueprint_execution)
    context = store.get_execution_context_from_id(sample_blueprint_execution.execution_id)
    assert context == sample_blueprint_execution.execution_context
    store.remove_effects()



def test_get_instruction(sample_execution_store_config, sample_blueprint_execution):
    store = DbBlueprintInstructionExecutionStore(sample_execution_store_config)
    store.remove_effects()
    store.rerun_migrations()
    store.store(sample_blueprint_execution)
    instruction_state = store.get_instruction_to_process('testcase_worker_id')
    assert instruction_state.status == InstructionStatus.PROCESSING
    store.remove_effects()
