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
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater
from admix.utils import make_did

@Collector
class MoveDataToRSE():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')


        #Define data types
        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        #Get other parameters
        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']
        self.periodic_check = helper.get_hostconfig()['upload_periodic_check']
        self.RSES = helper.get_hostconfig()['rses']

        self.FROMRSE = "UC_OSG_USERDISK"
        self.TORSE = "CCIN2P3_USERDISK"

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


    def add_conditional_rule(self,run_number, dtype, hash, from_rse, to_rse, lifetime=None, update_db=True):
        did = make_did(run_number, dtype, hash)
        result = self.rc.AddConditionalRule(did, from_rse, to_rse, lifetime=lifetime)
        #if result == 1:
        #   return
        helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: conditional rule added: {2} ---> {3}'.format(run_number,dtype,did,to_rse))

        if update_db:
            rucio_rule = self.rc.GetRule(did, rse=to_rse)
            data_dict = {'host': "rucio-catalogue",
                         'type': dtype,
                         'location': to_rse,
                         'lifetime': rucio_rule['expires'],
                         'status': 'transferring',
                         'did': did,
                         'protocol': 'rucio'
                     }
            self.db.db.find_one_and_update({'number': run_number},
                                      {'$set': {'status': 'transferring'}}
                                  )

            docid = self.db.db.find_one({'number': run_number}, {'_id': 1})['_id']
            self.db.AddDatafield(docid, data_dict)




    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')


#        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES + self.NORECORDS_DTYPES
        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES


        # Get all runs that are already transferred and that still have some data_types in eb 
        cursor = self.db.db.find({
#            'number': {"$lt": 8000, "$gte": 7900},
            'number': {"$gte": 7300},
#            'number': 7597,
            'data' : { "$elemMatch": { "host" : "rucio-catalogue" , "type" : {"$in" : data_types}, "location" : self.FROMRSE }},
            'status': 'transferred'
        },
        {'_id': 1, 'number': 1, 'data': 1})

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Runs that will be transferred are {0}'.format([c["number"] for c in cursor]))

        # Runs over all listed runs
        for run in cursor:
            number = run['number']
            helper.global_dictionary['logger'].Info('Treating run {0}'.format(number))
            for dtype in data_types:
                helper.global_dictionary['logger'].Info('\t==> Looking for data type {0}'.format(dtype))
                # get the datum for this datatype
                datum = None
                for d in run['data']:
                    if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == self.FROMRSE:
                        datum = d

                if datum is None:
                    helper.global_dictionary['logger'].Info('\t\t==> Data type {0} not in source RSE : {1}'.format(dtype,self.FROMRSE))
                    continue

                if 'did' not in datum:
                    helper.global_dictionary['logger'].Info('\t\t==> There is no did on the data type {1}'.format(dtype))
                    continue

                did = datum['did']

                hash = did.split('-')[-1]

                helper.global_dictionary['logger'].Info('\t\t==> Run {0}, data type {1}: found rule {2} in {3}'.format(number,dtype,did,self.FROMRSE))

                # check if a rule already exists in the destination RSE
                rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.TORSE)
                if rucio_rule['exists'] and rucio_rule['state'] == 'OK':
                    helper.global_dictionary['logger'].Info('\t\t==> Data already in the destination RSE : {0}'.format(self.TORSE))
                    continue

                helper.global_dictionary['logger'].Info('\t\t==> Run {0}, data type {1}: rule added: {2} ---> {3}'.format(number,dtype,did,self.TORSE))

                self.add_conditional_rule(number, dtype, hash, self.FROMRSE, self.TORSE)


        return 0


    def __del__(self):
        pass
