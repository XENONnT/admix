# -*- coding: utf-8 -*-
import json
import os
from admix.helper import helper
import time
import shutil
import psutil

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

#get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.utils import make_did
from admix.utils.list_file_replicas import list_file_replicas
import pymongo

@Collector
class Upload():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')

        open("/tmp/admix-upload_from_lngs", 'a').close()

        #Take all data types categories
        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.LIGHT_RAW_RECORDS_DTYPES = helper.get_hostconfig()['light_raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        # Choose which RSE you want upload to
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        #Choose which data type you want to treat
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES + self.LIGHT_RAW_RECORDS_DTYPES

        if helper.global_dictionary.get('high'):
               self.DTYPES = self.NORECORDS_DTYPES + self.LIGHT_RAW_RECORDS_DTYPES

        if helper.global_dictionary.get('low'):
               self.DTYPES = self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES


        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']
        self.periodic_check = helper.get_hostconfig()['upload_periodic_check']

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


    def get_dataset_to_upload_from_manager(self):
        process = psutil.Process()
        screen = process.parent().parent().parent().parent().cmdline()[-1]
        filename = "/tmp/admix-"+screen
        dataset = {}
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                dataset = json.load(f)
                f.close()

        if thread != {}:
            run = self.db.db.find_one({'number': dataset['number']}, {'number': 1, 'data': 1, 'bootstrax': 1})

            # Get run number
            number = run['number']

            # search the dtype that has to be uploaded
            for d in run['data']:
                if d['type'] == dataset['type'] and dataset['eb'] in d['host'] and dataset['hash'] in d['location']:
                    return run['_id'], d
                
        return 0,''


    def reset_upload_to_manager(self):
        process = psutil.Process()
        screen = process.parent().parent().parent().parent().cmdline()[-1]
        filename = "/tmp/admix-"+screen
        if os.path.isfile(filename):
            with open(filename, 'w') as f:
                json.dump({},f)
                f.close()


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

        # Get a new dataset to upload
        id_to_upload, datum = self.get_dataset_to_upload_from_manager()
        if id_to_upload == 0:
            helper.global_dictionary['logger'].Info('\t==> No data type available to upload')
            return 0

        # Get the run
        run = self.db.db.find_one({'_id': id_to_upload}, {'number': 1})

        # Building the did
        number = run['number']
        dtype = datum['type']
        file = datum['location'].split('/')[-1]
        hash = file.split('-')[-1]
        did = make_did(number, dtype, hash)
        eb = datum['host'].split('.')[0]
        helper.global_dictionary['logger'].Info('\t==> Uploading did {0} from host {1}'.format(did,eb))

        # Modify data type status to "transferring"
        self.db.db.find_one_and_update({'_id': id_to_upload, 'data.type' : datum['type'], 'data.location' : datum['location'], 'data.host' : datum['host'] },
                          { '$set': { "data.$.status" : "transferring" } })

        # Check, for coherency, if there is no rucio entry in DB for this data type
        in_rucio_upload_rse = False
        in_rucio_somewhere_else = False
        for d in run['data']:
            if d['type'] == datum['type'] and d['host'] == 'rucio-catalogue' and hash in d['did'] and d['location'] == self.UPLOAD_TO:
                in_rucio_upload_rse = True
            if d['type'] == datum['type'] and d['host'] == 'rucio-catalogue' and hash in d['did'] and d['location'] != self.UPLOAD_TO:
                in_rucio_somewhere_else = True
        if in_rucio_upload_rse:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1} has already a DB entry for RSE {2}. Forced to stop'.format(number,dtype,self.UPLOAD_TO))
            return 0
        if in_rucio_somewhere_else:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1} has already a DB entry for some external RSE. Forced to stop'.format(number,dtype))
            return 0

        # Querying Rucio: if a rule exists already for this DID on LNGS, skip uploading
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if rucio_rule['exists']:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1} has already a Rucio rule for RSE {2}. Forced to stop'.format(number,dtype,self.UPLOAD_TO))
            return 0
            
        # Building the full path of data to upload
        upload_path = os.path.join(self.DATADIR, eb, file)

        # Finally, start uploading with Rucio
        result = self.rc.Upload(did, upload_path, self.UPLOAD_TO, lifetime=None)
        helper.global_dictionary['logger'].Info('\t==> Uploading did {0} from host {1} done'.format(did,eb))

        # Wait for 10 seconds
        time.sleep(10)
  
        # Checking the status of this new upload rule
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if rucio_rule['state'] != 'OK':
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}, according to Rucio, uploading failed. Forced to stop'.format(number,dtype))
            exit()

        # Modify data type status to "transferred"
        self.db.db.find_one_and_update({'_id': id_to_upload, 'data.type' : datum['type'], 'data.location' : datum['location'], 'data.host' : datum['host'] },
                          { '$set': { "data.$.status" : "transferred" } })

        # Add a new data field with LNGS as RSE and with status "trasferred"
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
            if dtype in self.NORECORDS_DTYPES+self.LIGHT_RAW_RECORDS_DTYPES:
                self.add_rule(number, dtype, hash, 'UC_DALI_USERDISK',datum=datum)
                if dtype in self.LIGHT_RAW_RECORDS_DTYPES:
                    self.add_conditional_rule(number, dtype, hash, 'UC_DALI_USERDISK', 'SURFSARA_USERDISK', datum=datum)
            elif dtype in self.RECORDS_DTYPES:
                self.add_rule(number, dtype, hash, 'UC_OSG_USERDISK',datum=datum)
                self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'SURFSARA_USERDISK',datum=datum)
            elif dtype in self.RAW_RECORDS_DTYPES:
                self.add_rule(number, dtype, hash, 'UC_OSG_USERDISK',datum=datum)
                self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'SURFSARA_USERDISK', datum=datum)

        # unbook the did
        self.reset_upload_to_manager()

        return 0


    def __del__(self):
        pass
