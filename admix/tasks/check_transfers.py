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
#            {'number':7185},
            {'number': 1, 'data': 1})

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Check transfers : checking status of {0} runs'.format(len(cursor)))

        for run in list(cursor):
            # for each run, check the status of all REPLICATING rules
            rucio_stati = []
            for d in run['data']:
                if d['host'] == 'rucio-catalogue':
#                    if run['number']==7695 and d['status'] == 'error':
#                        self.db.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': d}},
#                                                       {'$set': {'data.$.status': 'transferring'}}
#                                                   )                        
                    if d['status'] != 'transferring':
                        rucio_stati.append(d['status'])
                    else:
                        did = d['did']
                        status = self.rc.CheckRule(did, d['location'])
                        if status == 'REPLICATING':
                            rucio_stati.append('transferring')
                        elif status == 'OK':
                            # update database
                            helper.global_dictionary['logger'].Info('Check transfers : updating DB for run {0}, dtype {1}, location {2}'.format(run['number'], d['type'],d['location']))
                            self.db.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': d}},
                                                      {'$set': {'data.$.status': 'transferred'}}
                            )
                            rucio_stati.append('transferred')

                        elif status == 'STUCK':
                            self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': d}},
                                                      {'$set': {'data.$.status': 'error'}}
                            )
                            rucio_stati.append('error')

                            # are there any other rucio rules transferring?
            if len(rucio_stati) > 0 and all([s == 'transferred' for s in rucio_stati]):
                self.db.SetStatus(run['number'], 'transferred')






    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')


        # Check transfers
        self.check_transfers()
        

        return 0


    def __del__(self):
        pass
