from rucio.client.client import Client
from rucio.client.replicaclient import ReplicaClient
from rucio.client.accountclient import AccountClient
from rucio.client.rseclient import RSEClient
from rucio.client.downloadclient import DownloadClient
from rucio.client.uploadclient import UploadClient
from rucio.client.ruleclient import RuleClient
from rucio.client.didclient import DIDClient

from . import logger


rucio_client = None
replica_client = None
account_client = None
rse_client = None
download_client = None
upload_client = None
rule_client = None
did_client = None

def _init_clients():
    global rucio_client
    global replica_client
    global account_client
    global rse_client
    global download_client
    global upload_client
    global rule_client
    global did_client

    rucio_client = Client()
    replica_client = ReplicaClient()
    account_client = AccountClient()
    rse_client = RSEClient()
    download_client = DownloadClient(logger=logger)
    upload_client = UploadClient()
    rule_client = RuleClient()
    did_client = DIDClient()

def needs_client(func):
    def wrapped(*args, **kwargs):
        if rucio_client is None:
            _init_clients()
        return func(*args, **kwargs)
    return wrapped
