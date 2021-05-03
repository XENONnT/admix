# -*- coding: utf-8 -*-
import json
import os
from admix.helper import helper
import time
import shutil

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

#get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.utils import make_did

@Collector
class CheckTransfers():
    """
    Using the runDB, it searches for all runs and all data types for which a Rucio rule is ongoing
    (identified by both status specific data.status equal to "transferring".
    For each of them, it checks, by using Rucio API commands, if those rules have been
    succesfully completed. If so, the corresponding data.status is updated as
    "transferred". If all data types are flagged as transferred, then the run itself
    is flagged as "transferred".
    """

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')

        #Take all data types categories
        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        # Choose which RSE you want upload to
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        #Choose which data type you want to treat
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES


        #Define the waiting time (seconds)
        self.waitfor = 60*5

        #Init the runDB
        self.db = ConnectMongoDB()

        #Init Rucio for later uploads and handling:
        self.rc = RucioSummoner(helper.get_hostconfig("rucio_backend"))
        self.rc.SetRucioAccount(helper.get_hostconfig('rucio_account'))
        self.rc.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        self.rc.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        self.rc.SetHost(helper.get_hostconfig('host'))
        self.rc.ConfigHost()
        self.rc.SetProxyTicket("rucio_x509")


    def check_transfers(self):
        cursor = self.db.db.find(
            {'status': 'transferring'},
#            {'number':10161},
            {'number': 1, 'data': 1, 'bootstrax': 1})

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Check transfers : checking status of {0} runs'.format(len(cursor)))

        for run in list(cursor):

            # Extracts the correct Event Builder machine who processed this run
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]

            # for each run, check the status of all REPLICATING rules
            rucio_stati = []
            eb_still_to_be_uploaded = []

            for d in run['data']:
                if d['host'] == 'rucio-catalogue':
#                    if run['number']==7695 and d['status'] == 'stuck':
#                        self.db.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': d}},
#                                                       {'$set': {'data.$.status': 'transferring'}}
#                                                   )                        
                    if d['status'] in ['transferring','error','stuck']:
                        did = d['did']
                        status = self.rc.CheckRule(did, d['location'])
                        if status == 'REPLICATING':
                            rucio_stati.append('transferring')
                        elif status == 'OK':
                            # update database
                            helper.global_dictionary['logger'].Info('Check transfers : Run {0}, data type {1}, location {2}: transferred'.format(run['number'], d['type'],d['location']))
                            self.db.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': d}},
                                                      {'$set': {'data.$.status': 'transferred'}}
                            )
                            rucio_stati.append('transferred')

                        elif status == 'STUCK':
                            self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': d}},
                                                      {'$set': {'data.$.status': 'stuck'}}
                            )
                            rucio_stati.append('stuck')
                    else:
                        rucio_stati.append(d['status'])


                # search if dtype still has to be uploaded
                if eb in d['host'] and d['type'] in self.DTYPES:
                    if 'status' not in d:
                        eb_still_to_be_uploaded.append(d['type'])
                    else:
                        if d['status'] != "transferred":
                            eb_still_to_be_uploaded.append(d['type'])

            # are there any other rucio rules transferring?
#            print(run['number'],eb_still_to_be_uploaded,rucio_stati)
            if len(rucio_stati) > 0 and all([s == 'transferred' for s in rucio_stati]) and len(eb_still_to_be_uploaded)==0:
                self.db.SetStatus(run['number'], 'transferred')
                helper.global_dictionary['logger'].Info('Check transfers : Run {0} fully transferred'.format(run['number']))





    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')


        # Check transfers
        self.check_transfers()
        

        return 0


    def __del__(self):
        pass
