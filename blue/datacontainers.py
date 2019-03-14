from enum import auto
from typing import List, Dict, Optional

from dataclasses import dataclass, field

from blue.base import Action, Adapter
from blue.util import generate_random_id, AutoNameEnum


@dataclass
class Event:
    topic: str
    metadata: Optional[Dict] = field(default_factory=dict)
    body: Optional[Dict] = field(default_factory=dict)

@dataclass
class BlueprintInstructionOutcome:
    action: Action
    adapter: Adapter


@dataclass
class BlueprintInstruction:
    conditions: List[str]
    outcome: BlueprintInstructionOutcome

class InstructionStatus(AutoNameEnum):
    IDLE = auto()
    PROCESSING = auto()
    COMPLETE = auto()
    FAILED = auto()

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