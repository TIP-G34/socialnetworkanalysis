from storage import Storage
from kuzu import Connection
import kuzu

class SocialNetworkAnalysis:
    storage: Storage

    def __init__(self, kuzu_client: Connection):
        self.storage = Storage(kuzu_client)