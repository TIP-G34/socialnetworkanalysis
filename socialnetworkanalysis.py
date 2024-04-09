from storage import Storage
from pycozo.client import Client

class SocialNetworkAnalysis:

    def __init__(self, cozo_client: Client):
        self.storage = Storage(cozo_client)