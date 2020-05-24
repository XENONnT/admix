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
        self.DTYPES = helper.get_hostconfig()['rawtype']
        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']
        self.periodic_check = helper.get_hostconfig()['upload_periodic_check']
        self.RSES = helper.get_hostconfig()['rses']

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


        # Get runs list to clean
        cursor = self.db.db.find({
#            'number': {"$lt": 7330},
#            'number': {"$gt": 7300}
            'number': 7319
        },
        {'_id': 1, 'number': 1, 'data': 1})

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Runs that will be processed are {0}'.format([c["number"] for c in cursor]))

        # Runs over all listed runs
        for run in cursor:
            number = run['number']
            helper.global_dictionary['logger'].Info('Treating run {0}'.format(number))
            for dtype in self.DTYPES:
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

                # check if a rule already exists for this DID on any RSE
                rses_with_rule = []
                for rse in self.RSES:
                    rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
                    if rucio_rule['exists']:
                        if "LNGS_USERDISK"==rucio_rule['rse']:
                            continue
                        rses_with_rule.append(rucio_rule['rse'])
                helper.global_dictionary['logger'].Info('\t==> Found in : {0}'.format(rses_with_rule))

                minimum_number_acceptable_rses = 2

                if len(rses_with_rule)>=minimum_number_acceptable_rses:

                    helper.global_dictionary['logger'].Info('\t==> Deleted') 

                    print(run['_id'],datum['type'],datum['host'])
                    self.db.RemoveDatafield(run['_id'],datum)
                    full_path = os.path.join(self.DATADIR, file)
                    print(full_path)
                    shutil.rmtree(full_path)

        return 0


    def __del__(self):
        pass
