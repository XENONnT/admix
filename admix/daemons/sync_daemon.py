import time
from admix.manager import synchronize
from .admix_daemon import AdmixDaemon


class SyncDaemon(AdmixDaemon):
    """Syncs the RunDB with Rucio"""
    def do_task(self, rundoc):
        synchronize(rundoc['number'])
