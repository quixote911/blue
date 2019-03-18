from blue.impl.db import DbBlueprintInstructionExecutionStore
from fixtures import *


def test_db_store(sample_execution_store_config, sample_blueprint_execution):
    store = DbBlueprintInstructionExecutionStore(sample_execution_store_config)
    store.remove_effects()
    store.rerun_migrations()
    store.store(sample_blueprint_execution)
    store.remove_effects()

