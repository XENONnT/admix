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

#        open("/tmp/admix-upload_from_lngs", 'a').close()

        #Take all data types categories
        self.RAW_RECORDS_TPC_TYPES = helper.get_hostconfig()['raw_records_tpc_types']
        self.RAW_RECORDS_MV_TYPES = helper.get_hostconfig()['raw_records_mv_types']
        self.RAW_RECORDS_NV_TYPES = helper.get_hostconfig()['raw_records_nv_types']
        self.LIGHT_RAW_RECORDS_TPC_TYPES = helper.get_hostconfig()['light_raw_records_tpc_types']
        self.LIGHT_RAW_RECORDS_MV_TYPES = helper.get_hostconfig()['light_raw_records_mv_types']
        self.LIGHT_RAW_RECORDS_NV_TYPES = helper.get_hostconfig()['light_raw_records_nv_types']
        self.HIGH_LEVEL_TYPES = helper.get_hostconfig()['high_level_types']
        self.HEAVY_HIGH_LEVEL_TYPES = helper.get_hostconfig()['heavy_high_level_types']
        self.RECORDS_TYPES = helper.get_hostconfig()['records_types']

        # Get the sequence of rules to be created according to the data type
        self.RAW_RECORDS_TPC_RSES = helper.get_hostconfig()["raw_records_tpc_rses"]
        self.RAW_RECORDS_MV_RSES = helper.get_hostconfig()["raw_records_mv_rses"]
        self.RAW_RECORDS_NV_RSES = helper.get_hostconfig()["raw_records_nv_rses"]
        self.LIGHT_RAW_RECORDS_TPC_RSES = helper.get_hostconfig()["light_raw_records_tpc_rses"]
        self.LIGHT_RAW_RECORDS_MV_RSES = helper.get_hostconfig()["light_raw_records_mv_rses"]
        self.LIGHT_RAW_RECORDS_NV_RSES = helper.get_hostconfig()["light_raw_records_nv_rses"]
        self.HIGH_LEVEL_RSES = helper.get_hostconfig()["high_level_rses"]
        self.HEAVY_HIGH_LEVEL_RSES = helper.get_hostconfig()["heavy_high_level_rses"]
        self.RECORDS_RSES = helper.get_hostconfig()["records_rses"]

        # Choose which RSE you want upload to
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        # Choose where is the main path of data to be upload
        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']

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

        # Get the current screen session
        process = psutil.Process()
        screen = process.parent().parent().parent().parent().cmdline()[-1]

        # Load the tmp file for this session written by the upload manager
        filename = "/tmp/admix-"+screen
        dataset = {}
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                try:
                    dataset = json.load(f)
                except json.decoder.JSONDecodeError:
                    pass
                f.close()

        if dataset != {}:
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


    def add_rule(self,run_number, dtype, hash, from_rse, to_rse, datum=None, lifetime=None, update_db=True):
        did = make_did(run_number, dtype, hash)
        if dtype in self.HIGH_LEVEL_TYPES:
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
#        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')

        # Get a new dataset to upload
        id_to_upload, datum = self.get_dataset_to_upload_from_manager()
        if id_to_upload == 0:
#            helper.global_dictionary['logger'].Info('\t==> No data type available to upload')
            return 0

        # Get the run
        run = self.db.db.find_one({'_id': id_to_upload}, {'number': 1, 'data': 1})

        # Get info from the screen session
        process = psutil.Process()
        screen = process.parent().parent().parent().parent().cmdline()[-1]

        # Building the did
        number = run['number']
        dtype = datum['type']
        file = datum['location'].split('/')[-1]
        hash = file.split('-')[-1]
        did = make_did(number, dtype, hash)
        eb = datum['host'].split('.')[0]
        helper.global_dictionary['logger'].Info('\t==> Screen {0}. Uploading did {1} from host {2}'.format(screen,did,eb))

        # Modify data type status to "transferring"
        self.db.db.find_one_and_update({'_id': id_to_upload, 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
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
            helper.global_dictionary['logger'].Info('\t==> Screen {0}. Run {1}, data type {2} has already a DB entry for RSE {3}. Forced to stop'.format(screen,number,dtype,self.UPLOAD_TO))
            self.reset_upload_to_manager()
            return 0
        if in_rucio_somewhere_else:
            helper.global_dictionary['logger'].Info('\t==> Screen {0}. Run {1}, data type {2} has already a DB entry for some external RSE. Forced to stop'.format(screen,number,dtype))
            self.reset_upload_to_manager()
            return 0

        # Querying Rucio: if a rule exists already for this DID on LNGS, skip uploading
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if rucio_rule['exists']:
            helper.global_dictionary['logger'].Info('\t==> Screen {0}. Run {1}, data type {2} has already a Rucio rule for RSE {3}. Forced to stop'.format(screen,number,dtype,self.UPLOAD_TO))
            self.reset_upload_to_manager()
            return 0
            
        # Building the full path of data to upload
        upload_path = os.path.join(self.DATADIR, eb, file)

        # Finally, start uploading with Rucio
        result = self.rc.Upload(did, upload_path, self.UPLOAD_TO, lifetime=None)
        helper.global_dictionary['logger'].Info('\t==> Screen {0}. Uploading did {1} from host {2} done'.format(screen,did,eb))

        # Wait for 10 seconds
        time.sleep(10)
  
        # Checking the status of this new upload rule
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if rucio_rule['state'] != 'OK':
            helper.global_dictionary['logger'].Info('\t==> Screen {0}. Run {1}, data type {2}, according to Rucio, uploading failed. Forced to stop'.format(screen, number,dtype))
            exit()

        # Modify data type status to "transferred"
        self.db.db.find_one_and_update({'_id': id_to_upload, 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
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

            rses = [self.UPLOAD_TO]

            if dtype in self.RAW_RECORDS_TPC_TYPES:
                rses = rses + self.RAW_RECORDS_TPC_RSES
            if dtype in self.RAW_RECORDS_MV_TYPES:
                rses = rses + self.RAW_RECORDS_MV_RSES
            if dtype in self.RAW_RECORDS_NV_TYPES:
                rses = rses + self.RAW_RECORDS_NV_RSES

            if dtype in self.LIGHT_RAW_RECORDS_TPC_TYPES:
                rses = rses + self.LIGHT_RAW_RECORDS_TPC_RSES
            if dtype in self.LIGHT_RAW_RECORDS_MV_TYPES:
                rses = rses + self.LIGHT_RAW_RECORDS_MV_RSES
            if dtype in self.LIGHT_RAW_RECORDS_NV_TYPES:
                rses = rses + self.LIGHT_RAW_RECORDS_NV_RSES

            if dtype in self.HIGH_LEVEL_TYPES:
                rses = rses + self.HIGH_LEVEL_RSES

            if dtype in self.HEAVY_HIGH_LEVEL_TYPES:
                rses = rses + self.HEAVY_HIGH_LEVEL_RSES

            if dtype in self.RECORDS_TYPES:
                rses = rses + self.RECORDS_RSES

            for from_rse, to_rse in zip(rses, rses[1:]):
                to_rule = self.rc.GetRule(upload_structure=did, rse=to_rse)
                if not to_rule['exists']:
                    self.add_rule(number, dtype, hash, from_rse, to_rse, datum=datum)

        # unbook the did
        self.reset_upload_to_manager()

        return 0


    def __del__(self):
        pass
