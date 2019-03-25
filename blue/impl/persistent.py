import json
import logging
from typing import Dict, Optional

import boto3
from dataclasses import asdict
from peewee import Model, CharField, Proxy, DoesNotExist
from playhouse.postgres_ext import PostgresqlExtDatabase, JSONField

from blue.base import BlueprintInstructionExecutionStore, BlueprintExecution, BlueprintInstructionState, InstructionStatus, BlueprintInstruction, EventBus, \
    Event
from blue.blueprint import BlueprintManager
from blue.util import blue_json_dumps, superjson

database_proxy = Proxy()  # Create a proxy for our db.

log = logging.getLogger(__name__)

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


class EventModel(BaseModel):
    topic = CharField()
    metadata = JSONField(dumps=blue_json_dumps)
    body = JSONField(dumps=blue_json_dumps)


class PersistentEventBus(EventBus):
    def __init__(self, config):
        super().__init__(config)
        self.db = PostgresqlExtDatabase(**config['db'])
        database_proxy.initialize(self.db)
        self._migrations()

    def _migrations(self):
        self.db.create_tables([EventModel], safe=True)

    def publish(self, event: Event):
        exec_id = event.metadata['blueprint_execution_id']
        existing_event = self.get_event(event.topic, exec_id)
        if not existing_event:
            newevent = EventModel(topic=event.topic, body=event.body, metadata=event.metadata)
            newevent.save()
        else:
            existing_event.body = event.body
            existing_event.metadata = event.metadata
            existing_event.save()

    def get_event(self, topic: str, blueprint_execution_id: str):
        try:
            eventmodel = EventModel.get((EventModel.topic == topic) & (EventModel.metadata['blueprint_execution_id'] == blueprint_execution_id))
            return eventmodel
        except DoesNotExist as e:
            return




class PersistentBlueprintInstructionExecutionStore(BlueprintInstructionExecutionStore):

    def __init__(self, manager: BlueprintManager, config):
        super().__init__(manager, config)
        self.config = config
        self.db = PostgresqlExtDatabase(**config['db'])
        self._initialize()
        self._migrations()
        self.receipthandle_by_instructionstateid = dict()

    def remove_effects(self):
        self.sqs.delete_queue(QueueUrl=self._queue_url)
        self.db.drop_tables([BlueprintExecutionModel, BlueprintInstructionStateModel], safe=True)

    def _initialize(self):
        database_proxy.initialize(self.db)
        sqs_region = self.config.get('sqs', {}).get('region_name')
        kwargs = {} if not sqs_region else dict(region_name=sqs_region)
        self.sqs = boto3.client('sqs', **kwargs)

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

    def _get_instruction_to_process(self, worker_id) -> Optional[BlueprintInstructionState]:
        response = self.sqs.receive_message(QueueUrl=self._queue_url, MaxNumberOfMessages=1)
        messages = response.get('Messages')
        if not messages:
            return

        b = json.loads(messages[0]['Body'])
        self.receipthandle_by_instructionstateid[b['id_']] = messages[0]['ReceiptHandle']
        return BlueprintInstructionState(
            instruction=self.manager.objectify_instruction(b['instruction']),
            blueprint_execution_id=b['blueprint_execution_id'],
            status=InstructionStatus(b['status']),
            id_=b['id_']
        )

    def _remove_from_queue(self, instruction_state: BlueprintInstructionState):
        self.sqs.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self.receipthandle_by_instructionstateid[instruction_state.id_]
        )

    def _set_status_for_instruction(self, instruction_state: BlueprintInstructionState, status: InstructionStatus):
        log.info(f"Setting instruction_state {instruction_state.id_}'s status to be {status}")
        terminal_states = [InstructionStatus.COMPLETE, InstructionStatus.FAILED]
        instruction_state.status = status
        BlueprintInstructionStateModel.update(status=status.value).where(BlueprintInstructionStateModel.instruction_state_id==instruction_state.id_).execute()
        if instruction_state.status in terminal_states:
            self._remove_from_queue(instruction_state)
        else:
            # We're relying on the visibility timeout to retry
            pass

    def get_execution_context_from_id(self, blueprint_execution_id) -> Dict:
        model: BlueprintExecutionModel = BlueprintExecutionModel.select().where(BlueprintExecutionModel.execution_id == blueprint_execution_id).get()
        return dict(model.execution_context)
