from abc import ABC, abstractmethod
from enum import auto
from typing import Optional, Dict, Type, List

from dataclasses import dataclass, field, asdict
from blue.util import AutoNameEnum, generate_random_id


class BlueError(Exception):
    pass


# ------- SERVICES BASE -------- #

@dataclass
class Event:
    topic: str
    metadata: Optional[Dict] = field(default_factory=dict)
    body: Optional[Dict] = field(default_factory=dict)


class EventBus(ABC):
    def __init__(self, config):
        pass

    @abstractmethod
    def publish(self, event: Event):
        pass

    @abstractmethod
    def get_event(self, topic):
        pass


class Action(ABC):
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus

    @abstractmethod
    def act(self, input):
        pass


class Adapter(ABC):
    @abstractmethod
    def adapt(self, context, events):
        pass


# ------- BLUEPRINT BASE -------- #

class InstructionStatus(AutoNameEnum):
    IDLE = auto()
    PROCESSING = auto()
    COMPLETE = auto()
    FAILED = auto()


@dataclass
class BlueprintInstructionOutcome:
    action: Type[Action]
    adapter: Type[Adapter]


@dataclass
class BlueprintInstruction:
    conditions: List[str]
    outcome: BlueprintInstructionOutcome


@dataclass
class BlueprintInstructionState:
    instruction: BlueprintInstruction
    blueprint_execution_id: str
    status: InstructionStatus = InstructionStatus.IDLE
    id_: str = field(default_factory=generate_random_id)


@dataclass
class Blueprint:
    name: str
    instructions: List[BlueprintInstruction]


@dataclass
class BlueprintExecution:
    execution_id: str
    execution_context: Dict
    blueprint: Blueprint
    instructions_states: List[BlueprintInstructionState]


class BlueprintInstructionExecutionStore(ABC):
    def __init__(self, config):
        pass

    @abstractmethod
    def _store_blueprint_execution(self, blueprint_execution: BlueprintExecution):
        pass

    @abstractmethod
    def _store_instruction_state(self, instr_state: BlueprintInstructionState):
        pass

    def store(self, blueprint_execution: BlueprintExecution):
        self._store_blueprint_execution(blueprint_execution)
        for instr_state in blueprint_execution.instructions_states:
            self._store_instruction_state(instr_state)

    def get_instruction_to_process(self, worker_id=None) -> Optional[BlueprintInstructionState]:
        instruction_state = self._get_instruction_to_process(worker_id)
        if not instruction_state:
            return
        self._set_status_for_instruction(instruction_state, InstructionStatus.PROCESSING)
        return instruction_state

    def acknowledge_success(self, instruction_state: BlueprintInstructionState):
        self._set_status_for_instruction(instruction_state, InstructionStatus.COMPLETE)

    def requeue(self, instruction_state: BlueprintInstructionState):
        self._set_status_for_instruction(instruction_state, InstructionStatus.IDLE)

    def report_failure(self, instruction_state: BlueprintInstructionState):
        self._set_status_for_instruction(instruction_state, InstructionStatus.FAILED)

    @abstractmethod
    def _get_instruction_to_process(self, worker_id) -> Optional[BlueprintInstructionState]:
        pass

    @abstractmethod
    def _set_status_for_instruction(self, instruction_state: BlueprintInstructionState, status: InstructionStatus):
        pass

    @abstractmethod
    def get_execution_context_from_id(self, blueprint_execution_id) -> Dict:
        pass
