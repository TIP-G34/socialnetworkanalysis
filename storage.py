from pycozo.client import Client

class Storage:

    def __init__(self, cozo_client: Client):
        self.cozo_client = cozo_client