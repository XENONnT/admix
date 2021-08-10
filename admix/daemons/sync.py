from admix.manager import synchronize
from .daemon import AdmixDaemon


class SyncDaemon(AdmixDaemon):
    """Syncs the RunDB with Rucio"""

    query = {'data': {'$elemMatch': {'status': 'transferring'}},
             'number': {'$gt': 10000}
             }

    desc = "Syncing rundb with rucio"

    def __init__(self, db_query=None, dtype=None):
        super().__init__(db_query)
        self.dtype = dtype

    def do_task(self, rundoc):
        synchronize(rundoc['number'], self.dtype)

