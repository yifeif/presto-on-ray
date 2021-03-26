from uuid import uuid4

import ray


class Worker:

    def __init__(self, config):
        self.node_id = uuid4()

    def run(self):
        pass


@ray.remote
class WorkerActor(Worker):
    """Actor that manages instance state. This actor centralizes all calls to the cloud provider
    and provides an API for Anyscale services to query about the state of instances.
    """
