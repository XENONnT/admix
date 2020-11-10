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
class UploadFromLNGSSingleThread():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')


        open("/tmp/admix-upload_from_lngs", 'a').close()

        #Take all data types categories
        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        # Choose which RSE you want upload to
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        #Choose which data type you want to treat
#        self.DTYPES = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES + self.NORECORDS_DTYPES
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES


        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']
        self.periodic_check = helper.get_hostconfig()['upload_periodic_check']

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



    def find_next_run_to_upload(self):
        cursor = self.db.db.find({'status': 'eb_ready_to_upload', 'bootstrax.state': 'done' }, {'number': 1, 'data': 1})
#        cursor = self.db.db.find({'status': 'uploading'}, {'number': 1, 'data': 1})
        id_run = 0
        min_run = float('inf')

        for run in cursor:
#            if run['number']<8500:
#                continue
#            if run['number'] not in [7155,7156,7218]:
#                 continue
            if run['number'] < min_run:
                min_run = run['number']
                id_run = run['_id']
        return id_run





    def add_rule(self,run_number, dtype, hash, rse, datum=None, lifetime=None, update_db=True):
        did = make_did(run_number, dtype, hash)
        if dtype in self.NORECORDS_DTYPES:
            priority = 1
        else:
            priority = 3
        result = self.rc.AddRule(did, rse, lifetime=lifetime, priority=priority)
        #if result == 1:
        #   return
        helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: rule added: {2} ---> {3}'.format(run_number,dtype,did,rse))

        if update_db:
            self.db.db.find_one_and_update({'number': run_number},
                                      {'$set': {'status': 'transferring'}}
                                  )

            rucio_rule = self.rc.GetRule(did, rse=rse)
            updated_fields = {'host': "rucio-catalogue",
                         'type': dtype,
                         'location': rse,
                         'lifetime': rucio_rule['expires'],
                         'status': 'transferring',
                         'did': did,
                         'protocol': 'rucio'
            }

            if datum == None:
                data_dict = updated_fields
            else:
                data_dict = datum.copy()
                data_dict.update(updated_fields)

            docid = self.db.db.find_one({'number': run_number}, {'_id': 1})['_id']
            self.db.AddDatafield(docid, data_dict)


    def add_conditional_rule(self,run_number, dtype, hash, from_rse, to_rse, datum=None, lifetime=None, update_db=True):
        did = make_did(run_number, dtype, hash)
        if dtype in self.NORECORDS_DTYPES:
            priority = 1
        else:
            priority = 3
        result = self.rc.AddConditionalRule(did, from_rse, to_rse, lifetime=lifetime, priority=priority)
        #if result == 1:
        #   return
        helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: conditional rule added: {2} ---> {3}'.format(run_number,dtype,did,to_rse))

        if update_db:
            self.db.db.find_one_and_update({'number': run_number},
                                      {'$set': {'status': 'transferring'}}
                                  )

            rucio_rule = self.rc.GetRule(did, rse=to_rse)
            updated_fields = {'host': "rucio-catalogue",
                         'type': dtype,
                         'location': to_rse,
                         'lifetime': rucio_rule['expires'],
                         'status': 'transferring',
                         'did': did,
                         'protocol': 'rucio'
            }

            if datum == None:
                data_dict = updated_fields
            else:
                data_dict = datum.copy()
                data_dict.update(updated_fields)

            docid = self.db.db.find_one({'number': run_number}, {'_id': 1})['_id']
            self.db.AddDatafield(docid, data_dict)





    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')

#        number = 8113
#        dtype = "raw_records"
#        hash = "rfzvpzj4mf"
#        self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'CCIN2P3_USERDISK')
#        return 0

        # Get a new run to upload
        id_to_upload = self.find_next_run_to_upload()
        if id_to_upload == 0:
             helper.global_dictionary['logger'].Info('No run available to upload')
             return 0
        run = self.db.db.find_one({'_id': id_to_upload}, {'number': 1, 'data': 1, 'bootstrax': 1})

        # Gets run number
        number = run['number']

        # Books the run by setting its status to the PID of the process
        PID = str(os.getpid())
        self.db.SetStatus(number, PID)
#        self.db.SetStatus(number, 'uploading')

        # Wait for 10 seconds
        time.sleep(10)

        # Then check if status is still equal to the same PID
        run = self.db.db.find_one({'_id': id_to_upload}, {'status': 1, 'number': 1, 'data': 1, 'bootstrax': 1})
        if run['status'] != PID:
            helper.global_dictionary['logger'].Info('Run {0} already taken by another process'.format(number))
            return 0

        # If yes, then book it by setting its status to 'uploading'
        self.db.SetStatus(number, 'uploading')


        # Extracts the correct Event Builder machine who processed this run
        bootstrax = run['bootstrax']
        eb = bootstrax['host'].split('.')[0]

        # Performs upload on selected run
        helper.global_dictionary['logger'].Info('Uploading run {0} from {1}'.format(number,eb))

        # loop on all data types we want to upload
        for dtype in self.DTYPES:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: uploading'.format(number,dtype))

            # get the datum for this datatype
            datum = None
            in_rucio_upload_rse = False
            in_rucio_somewhere_else = False
            for d in run['data']:
                if d['type'] == dtype and eb in d['host']:
                    datum = d

                if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == self.UPLOAD_TO:
                    in_rucio_upload_rse = True

                if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] != self.UPLOAD_TO:
                    in_rucio_somewhere_else = True

            if datum is None:
                helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: not found'.format(number,dtype))
                continue

            file = datum['location'].split('/')[-1]

            hash = file.split('-')[-1]

            upload_path = os.path.join(self.DATADIR, eb, file)

            # create a DID to upload
            did = make_did(number, dtype, hash)

            # check if a rule already exists for this DID on LNGS
            rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
#            helper.global_dictionary['logger'].Info('It was already in Rucio : {0}'.format(in_rucio_upload_rse))
#            helper.global_dictionary['logger'].Info('Rucio rule : {0}'.format(rucio_rule))
#            print("Did: ",did)
#            print("Upload path: ",upload_path)

            if in_rucio_somewhere_else:
                helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: cannot upload it because it is already on Rucio somewhere else'.format(number,dtype))
                continue

            # if not in rucio already (no matter where) and no rule exists, upload into rucio
            if not in_rucio_upload_rse and not rucio_rule['exists']:
                result = self.rc.Upload(did,
                                        upload_path,
                                        self.UPLOAD_TO,
                                        lifetime=None)
                helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}: uploaded ({2})'.format(number,dtype,did))
                
                # get the status of this new upload rule
                rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)

                # if the rule status is OK, then update the DB
                if rucio_rule['state'] == 'OK':
                    data_dict = datum.copy()
                    data_dict.update({'host': "rucio-catalogue",
                                 'type': dtype,
                                 'location': self.UPLOAD_TO,
                                 'lifetime': rucio_rule['expires'],
                                 'status': 'transferred',
                                 'did': did,
                                 'protocol': 'rucio'
                             })
                    self.db.AddDatafield(run['_id'], data_dict)

            # set a rule to ship data on GRID
            if rucio_rule['state'] == 'OK':
                if dtype in self.NORECORDS_DTYPES:
                    self.add_rule(number, dtype, hash, 'UC_DALI_USERDISK',datum=datum)
                    self.add_conditional_rule(number, dtype, hash, 'UC_DALI_USERDISK', 'UC_OSG_USERDISK',datum=datum)
                else:
                    self.add_rule(number, dtype, hash, 'UC_OSG_USERDISK',datum=datum)
                    if dtype in self.RECORDS_DTYPES:
                        self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'UC_DALI_USERDISK',datum=datum)
                    self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'CCIN2P3_USERDISK',datum=datum)


        return 0


    def __del__(self):
        pass
