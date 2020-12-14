import os
from argparse import ArgumentParser
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.database import ConnectMongoDB
from admix.utils.naming import make_did
from admix.utils.list_file_replicas import list_file_replicas
from utilix.config import Config
import utilix
from bson.json_util import dumps
from datetime import timezone, datetime, timedelta
import pymongo

class Fix():

    def __init__(self):

        #Take all data types categories
        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        #Choose which data type you want to treat
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES

        #Init the runDB
        self.db = ConnectMongoDB()

        #Init Rucio for later uploads and handling:
        self.rc = RucioSummoner()


    def reset_upload(self,did):
        print("test:",did)



    def __del__(self):
        pass




    

def main():
    parser = ArgumentParser("admix-fix")

    config = Config()

    parser.add_argument("--number", type=int, help="Run number to fix", default=-1)
    parser.add_argument("--dtype", help="Data type to fix", default="")
    parser.add_argument("--did", help="DID to fix")
    parser.add_argument("--action", help="Which action you want to take")

    args = parser.parse_args()


    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))

    fix = Fix()

    try:
        if args.action == "reset_upload" and args.did:
            fix.reset_upload(args.did)
    except KeyboardInterrupt:
        return 0

