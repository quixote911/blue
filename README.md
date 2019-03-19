blue
------------------------------------

A Blueprint is a set of Instructions
    An Instruction is a Condition and an Outcome
        A Condition is a list of Event topics to listen to
        An Outcome specifes what happens when Condition is satisfied

A BlueprintExecution happens when a Blueprint is supplied with an initial Event and an execution_context

How to use:

+ Initial -
    - Let BlueprintManager know about Blueprint you're interested to execute
    - Let BlueprintExecutionManager know about BlueprintManager
+ Step 1 - Start a BlueprintExecution
    - Tell BlueprintExecutionManager to execute your Blueprint by supplying the initial Event and execution_context
    - This will queue up Instructions from your Blueprint to execute.

+ Step 2 - Execute the BlueprintExecution
    - Tell BlueprintExecutor about BlueprintExecutionManager.
    - Tell BlueprintExecutor to run. It will loop infinitely:
        - BlueprintExecutor gets all PENDING Instructions
        - It checks EventBus for Events satisfying the Instruction's Condition
        - If Condition Event is found:
            - It calls Outcome's Adapter with execution_context and Event.
            - Outcome.Adapter's result is fed as input to Outcome.Action
            - Instruction is marked SUCCESS
        - If not found:
            - It doesn't do anything and moves on to the next Instruction
            - Instruction is marked PENDING

