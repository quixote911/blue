from typing import Dict

from blue.base import BlueError
from blue.datacontainers import BlueprintInstructionOutcome, BlueprintInstruction, Blueprint


class InvalidBlueprintDefinition(BlueError):
    pass


class BlueprintManager:
    instruction_outcome_attribute_names = dict(BlueprintInstructionOutcome).keys()

    def __init__(self, config):
        BlueprintManager.validate_config(config)
        self.config = config
        self.live_blueprints_by_name = {}

    @staticmethod
    def validate_config(config):
        if 'namespace' not in config:
            raise InvalidBlueprintDefinition(f"Config must have 'namespace' key")
        for each in BlueprintManager.instruction_outcome_attribute_names:
            if each not in config['namespace']:
                raise InvalidBlueprintDefinition(f"Namespace must have key '{each}'")

    def _validate_blueprint_definition(self, blueprint_definition):
        if not blueprint_definition:
            raise InvalidBlueprintDefinition(f"Blueprint definition seems to be empty")

        if 'name' not in blueprint_definition:
            raise InvalidBlueprintDefinition(f"Blueprint definition must have key 'name'")

        blueprint_instructions = blueprint_definition.get('instructions')
        if not blueprint_instructions:
            raise InvalidBlueprintDefinition("Blueprint definition must have key 'instructions'")

        for i, instruction in enumerate(blueprint_instructions):
            if 'conditions' not in instruction or 'outcome' not in instruction:
                raise InvalidBlueprintDefinition(f"Instruction must have keys 'conditions' and 'outcome': {instruction}")
            for outcome_attribute_name in self.instruction_outcome_attribute_names:
                component_name_by_attribute_name = instruction['outcome']
                component_name = component_name_by_attribute_name.get(outcome_attribute_name)
                if not component_name:
                    raise InvalidBlueprintDefinition(
                        f"Instruction Outcome attribute '{outcome_attribute_name}' must exist inside instruction 'outcome' of {instruction}")

                component_object = self.config['namespace'][outcome_attribute_name].get(component_name)
                if not component_object:
                    raise InvalidBlueprintDefinition(
                        f"As per configured namespace {self.config}, no component is defined for attribute_name={outcome_attribute_name} componenet_name={component_name}")

    def _objectify_instruction(self, instruction_definition: Dict) -> BlueprintInstruction:
        def _objectify(attribute, component_name):
            return self.config['namespace'][attribute][component_name]

        outcome = BlueprintInstructionOutcome(
            action=_objectify('action', instruction_definition['outcome']['action']),
            adapter=_objectify('adapter', instruction_definition['outcome']['adapter'])
        )

        return BlueprintInstruction(conditions=instruction_definition['conditions'], outcome=outcome)

    def _convert_blueprint_definition_to_object(self, blueprint_definition) -> Blueprint:
        instructions = []
        for definition in blueprint_definition['instructions']:
            instruction: BlueprintInstruction = self._objectify_instruction(definition)
            instructions.append(instruction)
        return Blueprint(blueprint_definition['name'], instructions)

    def add_blueprint(self, blueprint_definition):
        self._validate_blueprint_definition(blueprint_definition)
        blueprint = self._convert_blueprint_definition_to_object(blueprint_definition)
        if blueprint.name in self.live_blueprints_by_name:
            raise InvalidBlueprintDefinition(f"Blueprint with name {blueprint_definition['name']} is already added")
        self.live_blueprints_by_name[blueprint.name] = blueprint