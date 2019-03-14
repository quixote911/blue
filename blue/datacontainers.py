from typing import List, Dict, Optional

from dataclasses import dataclass, field

from blue.base import Action, Adapter
from blue.util import generate_random_id



@dataclass
class Event:
    metadata: Dict
    body: Dict

@dataclass
class BlueprintInstructionOutcome:
    action: Action
    adapter: Adapter


@dataclass
class BlueprintInstruction:
    id_: str = field(default_factory=generate_random_id)
    conditions: List[str]
    outcome: BlueprintInstructionOutcome
    status: Optional[str]


@dataclass
class Blueprint:
    name: str
    instructions: List[BlueprintInstruction]


@dataclass
class BlueprintExecution:
    execution_id: str
    execution_context: Dict
    blueprint: Blueprint