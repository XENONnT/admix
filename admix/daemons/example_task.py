# -*- coding: utf-8 -*-
import json
import os
from admix.helper import helper

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

#get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater

@Collector
class RunExampleTask():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')


        #Define data types
        self.DTYPES = helper.get_hostconfig()['rawtype']
        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']
        self.periodic_check = helper.get_hostconfig()['upload_periodic_check']
#        self.DTYPES = ['raw_records', 'raw_records_he', 'raw_records_aqmon', 'raw_records_mv']
#        self.DATADIR = '/eb/ebdata'
#        self.periodic_check = 300

        #Init the runDB
        self.db = ConnectMongoDB()

        #We want the first and the last run:
        self.gboundary = self.db.GetBoundary()
        self.run_nb_min = self.gboundary['min_number']
        self.run_nb_max = self.gboundary['max_number']
        self.run_ts_min = self.gboundary['min_start_time']
        self.run_ts_max = self.gboundary['max_start_time']

        #Init the Rucio data format evaluator in three steps:
        self.rc_reader = ConfigRucioDataFormat()
        self.rc_reader.Config(helper.get_hostconfig('rucio_template'))

        #This class will evaluate your destinations:
        self.destination = Destination()

        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config(helper.get_hostconfig()['template'])

        #Init a class to handle keyword strings:
        self.keyw = Keyword()

        #Init Rucio for later uploads and handling:
        self.rc = RucioSummoner(helper.get_hostconfig("rucio_backend"))
        self.rc.SetRucioAccount(helper.get_hostconfig('rucio_account'))
        self.rc.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        self.rc.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        self.rc.SetHost(helper.get_hostconfig('host'))
        self.rc.ConfigHost()
        self.rc.SetProxyTicket("rucio_x509")
#        print(self.rc.Whoami())


    def GetPathsFromRunNumber(self, run_number, dtype, hash):

        print("Run "+str(run_number)+", data type "+dtype+", hash "+hash)
        
        cursor = self.db.GetRunByNumber(run_number)        
        if len(cursor)==0:
            return
        run = cursor[0]

        file_replicas = {}

        for d in run['data']:
            if d['type'] != dtype:
                continue
            if d['host'] != 'rucio-catalogue':
                continue
            if d['status'] != 'transferred':
                continue
#            if d['location'] != 'LNGS_USERDISK': #'UC_DALI_USERDISK':
            if d['location'] != 'UC_DALI_USERDISK':
                continue
            did = d['did']
            if did.split('-')[-1] != hash:
                continue
            if self.rc.CheckRule(did, d['location']) != 'OK':
                continue
            print("Found",did)
            file_replicas = self.rc.ListFileReplicas(did,d['location'],localpath=True)

        return list(file_replicas.values())


    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')

        paths = self.GetPathsFromRunNumber(7177,"raw_records","rfzvpzj4mf")
        print(paths)


        return 0

    def __del__(self):
        """Use this function to clean up your class settings if necessary"""

#        print('dummy stop')
        pass
