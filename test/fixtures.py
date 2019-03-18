import random
import string
import uuid

import copy
import pytest

from blue.base import BlueprintExecution, Blueprint, BlueprintInstruction, BlueprintInstructionOutcome, Action, Adapter, BlueprintInstructionState


def initialize_fixtures():
    pass

def get_random_string(N):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

testrunid = get_random_string(5)

class CheckForDeposit(Action):
    def act(self, input):
        print("Action - CheckedForDeposit")


class TransferToExchange(Action):
    def act(self, input):
        print("Action - CheckedForDeposit")


class BasicAdapter(Adapter):
    def adapt(self, context, events):
        return dict(foo='bar')


_data_sample_namespace_config = {
    'namespace': {
        'action': {
            'check_deposit': CheckForDeposit,
            'transfer_to_exchange': TransferToExchange
        },
        'adapter': {
            'basic_adapter': BasicAdapter
        }
    }
}
_data_sample_blueprint_definition = {
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

_data_blueprint_execution_id = 'test_blueprint_execution_id_1_' + testrunid

_data_sample_instruction_1 = BlueprintInstruction(
    conditions=['new_order'],
    outcome=BlueprintInstructionOutcome(
        action=CheckForDeposit,
        adapter=BasicAdapter
    )
)

_data_sample_instruction_2 = BlueprintInstruction(
    conditions=['deposit_status'],
    outcome=BlueprintInstructionOutcome(
        action=TransferToExchange,
        adapter=BasicAdapter
    )
)

_data_sample_blueprint = Blueprint(
    name='sample_blueprint',
    instructions=[_data_sample_instruction_1, _data_sample_instruction_2]
)

def get_data_sample_instruction_state_1():
    return BlueprintInstructionState(
    instruction=_data_sample_instruction_1,
    blueprint_execution_id=_data_blueprint_execution_id,
)

queue_prefix = 'test_'

_data_sample_execution_store_config = {
    'db': {
        'host': 'localhost',
        'port': 5432,
        'database': 'bluedata',
        'user': 'postgres',
        'password': 'postgres'
    },
    'sqs': {
        'prefix': queue_prefix
    }
}


@pytest.fixture(scope="package")
def sample_execution_store_config():
    return _data_sample_execution_store_config


@pytest.fixture(scope="package")
def sample_namespace_config():
    return _data_sample_namespace_config


@pytest.fixture(scope="package")
def _data_sample_blueprint_definition():
    return _data_sample_blueprint_definition


@pytest.fixture(scope="package")
def sample_instructions():
    return [_data_sample_instruction_1, _data_sample_instruction_2]


@pytest.fixture(scope="package")
def sample_blueprint():
    return _data_sample_blueprint


@pytest.fixture()
def sample_blueprint_execution():
    return BlueprintExecution(
        execution_id=_data_blueprint_execution_id,
        execution_context=dict(foo='bar'),
        blueprint=_data_sample_blueprint,
        instructions_states=[get_data_sample_instruction_state_1()]
    )
