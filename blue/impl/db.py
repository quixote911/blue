from typing import Dict

import boto3
from dataclasses import asdict
from peewee import Model, CharField, Proxy
from playhouse.postgres_ext import PostgresqlExtDatabase, JSONField

from blue.base import BlueprintInstructionExecutionStore, BlueprintExecution, BlueprintInstructionState, InstructionStatus
from blue.util import blue_json_dumps, superjson

database_proxy = Proxy()  # Create a proxy for our db.


class BaseModel(Model):
    class Meta:
        database = database_proxy  # Use proxy for our DB.


class BlueprintExecutionModel(BaseModel):
    execution_id = CharField(unique=True)
    execution_context = JSONField()
    blueprint = JSONField(dumps=blue_json_dumps)


class BlueprintInstructionStateModel(BaseModel):
    instruction_state_id = CharField(unique=True)
    blueprint_execution_id = CharField()
    instruction = JSONField(dumps=blue_json_dumps)
    status = CharField()


class DbBlueprintInstructionExecutionStore(BlueprintInstructionExecutionStore):

    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.db = PostgresqlExtDatabase(**config['db'])
        database_proxy.initialize(self.db)
        self.sqs = boto3.client('sqs')
        self._migrations()

    def remove_effects(self):
        self.db.drop_tables([BlueprintExecutionModel, BlueprintInstructionStateModel], safe=True)

    def rerun_migrations(self):
        self._migrations()

    def _get_queue_name(self):
        queue_name = 'BlueprintInstructionExecutionStore'
        return self.config['sqs'].get('prefix', '') + queue_name

    def _migrations(self):
        def does_queue_exist(queue_name):
            response = self.sqs.list_queues()
            for url in response.get('QueueUrls', []):
                if queue_name in url:
                    self._queue_url = url
                    return True
                return False

        queue_name = self._get_queue_name()
        self.db.create_tables([BlueprintExecutionModel, BlueprintInstructionStateModel], safe=True)
        if not does_queue_exist(queue_name):
            response = self.sqs.create_queue(QueueName=queue_name)
            self._queue_url = response['QueueUrl']

    def _store_blueprint_execution(self, blueprint_execution: BlueprintExecution):
        bem = BlueprintExecutionModel(execution_id=blueprint_execution.execution_id, execution_context=blueprint_execution.execution_context,
                                      blueprint=asdict(blueprint_execution.blueprint))
        bem.save()

    def _store_instruction_state(self, instruction_state: BlueprintInstructionState):
        instruction_definition = asdict(instruction_state.instruction)
        instr_state_model = BlueprintInstructionStateModel(instruction_state_id=instruction_state.id_,
                                                           blueprint_execution_id=instruction_state.blueprint_execution_id, instruction=instruction_definition,
                                                           status=instruction_state.status.value)
        instr_state_model.save()
        self.sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=superjson(instruction_state)
        )

    def get_instruction_to_process(self, worker_id) -> BlueprintInstructionState:
        pass

    def set_status_for_instruction(self, instruction_state: BlueprintInstructionState, state: InstructionStatus):
        pass

    def get_execution_context_from_id(self, blueprint_execution_id) -> Dict:
        model: BlueprintExecutionModel = BlueprintExecutionModel.select().where(BlueprintExecutionModel.execution_id == blueprint_execution_id).get()
        return dict(model.execution_context)
