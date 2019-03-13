from blue.datacontainers import BlueprintExecution


class BlueprintExecutionStore:
    def __init__(self, config):
        pass

    def store(self, blueprint_execution: BlueprintExecution):
        pass

    def get_execution_to_process(self, worker_id) -> BlueprintExecution:
        pass


class EventBus:
    def __init__(self, config):
        pass

    def publish(self, event):
        pass