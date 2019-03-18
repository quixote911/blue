import uuid

import pytest

from blue.base import BlueprintExecution, Blueprint, BlueprintInstruction, BlueprintInstructionOutcome, Action, Adapter

def initialize_fixtures():
    pass

class CheckForDeposit(Action):
    def act(self, input):
        print("Action - CheckedForDeposit")


class TransferToExchange(Action):
    def act(self, input):
        print("Action - CheckedForDeposit")


class BasicAdapter(Adapter):
    def adapt(self, context, events):
        return dict(foo='bar')


@pytest.fixture
def sample_namespace_config():
    return {
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


@pytest.fixture
def sample_blueprint_definition():
    return {
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


@pytest.fixture
def sample_instructions():
    return [
        BlueprintInstruction(
            conditions=['new_order'],
            outcome=BlueprintInstructionOutcome(
                action=CheckForDeposit,
                adapter=BasicAdapter
            )
        ),
        BlueprintInstruction(
            conditions=['deposit_status'],
            outcome=BlueprintInstructionOutcome(
                action=TransferToExchange,
                adapter=BasicAdapter
            )
        )
    ]


@pytest.fixture
def sample_blueprint():
    return Blueprint(
        name='sample_blueprint',
        instructions=sample_instructions()
    )


@pytest.fixture
def sample_blueprint_execution():
    BlueprintExecution(
        execution_id='test_blueprint_1',
        execution_context=dict(foo='bar'),
        blueprint=sample_blueprint()
    )
    return
