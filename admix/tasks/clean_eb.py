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
class CleanEB():

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

        self.minimum_number_acceptable_rses = 2

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





    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')


#        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES + self.NORECORDS_DTYPES
        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES


        # Get all runs that are already transferred and that still have some data_types in eb 
        cursor = self.db.db.find({
#            'number': {"$lt": 7330, "$gte": 7300}
            'number': {"$gte": 7330},
#            'number': 7448,
            'data' : { "$elemMatch": { "host" : {"$regex" : ".*eb.*"} , "type" : {"$in" : data_types}} },
            'status': 'transferred'
        },
        {'_id': 1, 'number': 1, 'data': 1})

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Runs that will be processed are {0}'.format([c["number"] for c in cursor]))

        # Runs over all listed runs
        for run in cursor:
            number = run['number']
            helper.global_dictionary['logger'].Info('Treating run {0}'.format(number))
            for dtype in data_types:
                helper.global_dictionary['logger'].Info('\t==> Looking for data type {0}'.format(dtype))
                # get the datum for this datatype
                datum = None
                for d in run['data']:
                    if d['type'] == dtype and 'eb' in d['host']:
                        datum = d

                if datum is None:
                    helper.global_dictionary['logger'].Info('Data type not in eb')
                    continue

                file = datum['location'].split('/')[-1]
                hash = file.split('-')[-1]

                # create a DID to upload
                did = make_did(number, dtype, hash)

                # check first with runDB if the data type already exists for this DID on any RSE
                rses_in_db = []
                for d in run['data']:
                    if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] != "LNGS_USERDISK":
                        rses_in_db.append(d['location'])
                helper.global_dictionary['logger'].Info('\t==> Found in following RSEs in the DB : {0}'.format(rses_in_db))
                if len(rses_in_db) < self.minimum_number_acceptable_rses:
                    continue

                # check if a rule already exists for this DID on any RSE
                rses_with_rule = []
                for rse in self.RSES:
                    rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
                    if rucio_rule['exists'] and rucio_rule['state'] == 'OK':
                        if "LNGS_USERDISK"==rucio_rule['rse']:
                            continue
                        rses_with_rule.append(rucio_rule['rse'])
                helper.global_dictionary['logger'].Info('\t==> Found in following RSEs : {0}'.format(rses_with_rule))


                if len(rses_with_rule)>=self.minimum_number_acceptable_rses:

#                    print(run['_id'],datum['type'],datum['host'])
                    self.db.RemoveDatafield(run['_id'],datum)
                    full_path = os.path.join(self.DATADIR, file)
#                    print(full_path)

                    helper.global_dictionary['logger'].Info('\t==> Deleted from DB') 

                    try:
                        shutil.rmtree(full_path)
                    except OSError as e:
                        helper.global_dictionary['logger'].Info('\t==> Error, cannot delete directory : {0}'.format(e))
                    else:
                        helper.global_dictionary['logger'].Info('\t==> Deleted from EB') 

        return 0


    def __del__(self):
        pass
