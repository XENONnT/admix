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
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        # Choose which RSE you want upload to
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        #Choose which data type you want to treat
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES

        if helper.global_dictionary.get('high'):
               self.DTYPES = self.NORECORDS_DTYPES

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


    def find_next_run_to_upload(self):
#        cursor = self.db.db.find({'status': 'eb_ready_to_upload', 'bootstrax.state': 'done' }, {'number': 1, 'data': 1})
        cursor = self.db.db.find({'status': { '$in': ['eb_ready_to_upload','transferring']}, 'bootstrax.state': 'done' }, {'number': 1, 'data': 1})
        id_run = 0
        min_run = float('inf')

        for run in cursor:
            print(run['number'])
            if run['number']<10000:
                continue
#            if run['number'] not in [10161]:
#                 continue
            if run['number'] < min_run:
                min_run = run['number']
                id_run = run['_id']
        print("   ",min_run)
        return id_run



    def find_next_run_and_dtype_to_upload(self):
        cursor = self.db.db.find({'status': { '$in': ['eb_ready_to_upload','transferring']}, 'bootstrax.state': 'done' }, {'number': 1, 'data': 1, 'bootstrax': 1}).sort('number',pymongo.ASCENDING)
        id_run = 0
        min_run = float('inf')

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('\t==> Runs in queue: {0}'.format(len(cursor)))

        for run in cursor:

            # Get run number
            number = run['number']

            # Forget about old runs
            if number<10000:
                continue

            # For debugging: select a specific run
#            if number not in [10161]:
#                continue

            # Extracts the correct Event Builder machine who processed this run
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]

            # Look for the first data type available to be uploaded
            datum = None
            for dtype in self.DTYPES:

                #if dtype != "raw_records_aqmon":
                #    continue

                # search if dtype still has to be uploaded
                for d in run['data']:
                    if d['type'] == dtype and eb in d['host'] and ('status' not in d or ('status' in d and d['status'] == 'eb_ready_to_upload')):
                        datum = d
                        continue
 
                if datum is not None:
                    continue

            # If there is a candidate data type, return run_id and data type
            if datum is not None:
                return run['_id'], datum

        return 0,''




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

        # Get a new run to upload
        id_to_upload, datum = self.find_next_run_and_dtype_to_upload()
        if id_to_upload == 0:
            helper.global_dictionary['logger'].Info('\t==> No data type available to upload')
            return 0

        # Load the run
        run = self.db.db.find_one({'_id': id_to_upload}, {'number': 1, 'data': 1, 'bootstrax': 1})

        # Get run number
        number = run['number']

        # Set run status to "transferring"
        self.db.SetStatus(number, 'transferring')

        # Extracts the correct Event Builder machine who processed this run
        bootstrax = run['bootstrax']
        eb = bootstrax['host'].split('.')[0]

#        # Performs upload on selected run
#        helper.global_dictionary['logger'].Info('Uploading run {0} from {1}'.format(number,eb))


        # Attempting to book this data type
        dtype = datum['type']
        helper.global_dictionary['logger'].Info('\t==> Run {0}, trying to book data type {1} for uploading. Starting match'.format(number,dtype))

        # Books the eb data entry by setting its status to the PID of the process
        PID = str(os.getpid())
        self.db.db.find_one_and_update({'_id': id_to_upload,'data': {'$elemMatch': datum}},
                                       {'$set': {'data.$.status': PID}})

        # Wait for 10 seconds
        time.sleep(10)

        # Then check if this status is still equal to the same PID
        run = self.db.db.find_one({'_id': id_to_upload}, {'number': 1, 'data': 1, 'bootstrax': 1})
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host'] and d['status']==PID:
                datum = d
                continue
        
        # If there is no data available to upload any more, exit
        if datum is None:
           helper.global_dictionary['logger'].Info('\t==> Run {0}, lost challenge, data type {1} not available any more'.format(number,dtype))
           return 0

        # Match won. Proceeding with uploading
        helper.global_dictionary['logger'].Info('\t==> Run {0}, match won, starting uploading data type {1}'.format(number,dtype))

        # Modify data type status to "transferring"
        self.db.db.find_one_and_update({'_id': id_to_upload,'data': {'$elemMatch': datum}},
                                       {'$set': {'data.$.status': "transferring"}})

        # Wait for 3 seconds
        time.sleep(3)

        # Reloading the updated datum
        run = self.db.db.find_one({'_id': id_to_upload}, {'number': 1, 'data': 1, 'bootstrax': 1})
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                continue

        # Check, for coherency, if there is no rucio entry in DB for this data type
        in_rucio_upload_rse = False
        in_rucio_somewhere_else = False
        for d in run['data']:
            if d['type'] == datum['type'] and d['host'] == 'rucio-catalogue' and d['location'] == self.UPLOAD_TO:
                in_rucio_upload_rse = True
            if d['type'] == datum['type'] and d['host'] == 'rucio-catalogue' and d['location'] != self.UPLOAD_TO:
                in_rucio_somewhere_else = True

        if in_rucio_upload_rse:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1} has already a DB entry for RSE {2}. Forced to stop'.format(number,dtype,self.UPLOAD_TO))
            return 0

        if in_rucio_somewhere_else:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1} has already a DB entry for some external RSE. Forced to stop'.format(number,dtype))
            return 0

        # Preparing relevant info for uploading
        
        file = datum['location'].split('/')[-1]
        hash = file.split('-')[-1]
        upload_path = os.path.join(self.DATADIR, eb, file)
        did = make_did(number, dtype, hash)

        # Querying Rucio: if a rule exists already for this DID on LNGS, skip uploading
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if rucio_rule['exists']:
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1} has already a Rucio rule for RSE {2}. Forced to stop'.format(number,dtype,self.UPLOAD_TO))
            return 0
            
        # Finally, start uploading with Rucio
        helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}, start uploading, DID = {2}'.format(number,dtype,did))
        result = self.rc.Upload(did, upload_path, self.UPLOAD_TO, lifetime=None)
        helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}, uploaded, DID = {2}'.format(number,dtype,did))

        # Wait for 10 seconds
        time.sleep(10)
  
        # Checking the status of this new upload rule
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if rucio_rule['state'] != 'OK':
            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}, according to Rucio, uploading failed. Forced to stop'.format(number,dtype))
            exit()

        # Checking the actual number of files uploaded
#        nfiles = len(list_file_replicas(number, dtype, hash, rucio_rule['rse']))
#        if nfiles != datum['file_count']:
#            helper.global_dictionary['logger'].Info('\t==> Run {0}, data type {1}, unconsistent number of files (Rucio: {2}, DB: {3}). Forced to stop'.format(number,dtype,nfiles,datum['file_count']))
#            exit()        

        # Update the eb data entry with status "transferred"
        self.db.db.find_one_and_update({'_id': id_to_upload,'data': {'$elemMatch': datum}},
                                       {'$set': {'data.$.status': "transferred"}})

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
            if dtype in self.NORECORDS_DTYPES:
                self.add_rule(number, dtype, hash, 'UC_DALI_USERDISK',datum=datum)
                self.add_conditional_rule(number, dtype, hash, 'UC_DALI_USERDISK', 'UC_OSG_USERDISK',datum=datum)
            else:
                self.add_rule(number, dtype, hash, 'UC_OSG_USERDISK',datum=datum)
#                if dtype in self.RECORDS_DTYPES:
#                    self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'UC_DALI_USERDISK',datum=datum)
                self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'CCIN2P3_USERDISK',datum=datum)
                if dtype in self.RAW_RECORDS_DTYPES:
                    self.add_conditional_rule(number, dtype, hash, 'UC_OSG_USERDISK', 'CNAF_TAPE2_USERDISK',datum=datum)


        return 0


    def __del__(self):
        pass
