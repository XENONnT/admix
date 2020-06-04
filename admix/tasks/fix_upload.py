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
class FixUpload():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')


        open("/tmp/admix-upload_from_lngs", 'a').close()

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


    def find_data_to_upload(self):
#        cursor = self.db.db.find({'status': 'needs_upload'}, {'number': 1, 'data': 1})
        cursor = self.db.db.find({'status': 'eb_ready_to_upload'}, {'number': 1, 'data': 1})
        ids = []

        for r in cursor:
            dtypes = set([d['type'] for d in r['data']])
            # check if all of the necessary data types are in the database
            if set(self.DTYPES) <= dtypes:
                ids.append(r['_id'])
        return ids




    def check_transfers(self):
        cursor = self.db.db.find(
            {'status': 'transferring'},
#            {'number':7185},
            {'number': 1, 'data': 1})

        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Checking transfer status of {0} runs'.format(len(cursor)))

        for run in list(cursor):
            # for each run, check the status of all REPLICATING rules
            rucio_stati = []
            for d in run['data']:
                if d['host'] == 'rucio-catalogue':
                    if d['status'] != 'transferring':
                        rucio_stati.append(d['status'])
                    else:
                        did = d['did']
                        status = self.rc.CheckRule(did, d['location'])
                        if status == 'REPLICATING':
                            rucio_stati.append('transferring')
                        elif status == 'OK':
                            # update database
                            helper.global_dictionary['logger'].Info('Updating DB for run {0}, dtype {1}'.format(run['number'], d['type']))
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



    def add_rule(self,run_number, dtype, hash, rse, lifetime=None, update_db=True):
        did = make_did(run_number, dtype, hash)
        result = self.rc.AddRule(did, rse, lifetime=lifetime)
        #if result == 1:
        #   return
        helper.global_dictionary['logger'].Info('Rule Added: {0} ---> {1}'.format(did,rse))

        if update_db:
            rucio_rule = self.rc.GetRule(did, rse=rse)
            data_dict = {'host': "rucio-catalogue",
                         'type': dtype,
                         'location': rse,
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



    def remove_from_eb(number, dtype):
        query = {'number': number}
        cursor = self.db.db.find_one(query, {'number': 1, 'data': 1})
        ebdict = None
        for d in cursor['data']:
            if '.xenon.local' in d['host'] and d['type'] == dtype:
                ebdict = d

        # get name of file (really directory) of this dtype in eb storage
        if ebdict is None:
            helper.global_dictionary['logger'].Info('No eventbuilder datum found for run {0} {1} Exiting.'.format(number,dtype))
            return

        file = ebdict['location'].split('/')[-1]
        path_to_rm = os.path.join(self.DATADIR, file)

        helper.global_dictionary['logger'].Info('{0}'.format(path_to_rm))
        helper.global_dictionary['logger'].Info('{0}'.format(ebdict))
        shutil.rmtree(path_to_rm)
        self.db.RemoveDatafield(cursor['_id'], ebdict)




    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')

        # type here the specific info of data to fix
        number = 7273
        dtype = "raw_records"
        hash = "rfzvpzj4mf"

        # List runs to upload
        run = self.db.db.find_one({
                            'number': number
                         },
                            {'number': 1, 'data': 1})

        helper.global_dictionary['logger'].Info('Fixing run {0}'.format(run["number"]))


        # Set the run number back to status "transferring"
        self.db.SetStatus(run['number'], 'transferring')

        # Performs uploads to all listed runs
        if 'number' in run:
            helper.global_dictionary['logger'].Info('Uploading run {0}'.format(number))
            if dtype in self.DTYPES:
                helper.global_dictionary['logger'].Info('\t==> Uploading {0}'.format(dtype))
                # get the datum for this datatype
                datum = None
                in_rucio = False
                for d in run['data']:
                    if d['type'] == dtype and 'eb' in d['host']:
                        datum = d

                    if d['type'] == dtype and d['host'] == 'rucio-catalogue':
                        in_rucio = True

                if datum is None:
                    helper.global_dictionary['logger'].Info('Data type {0} not found for run {1}'.format(dtype,number))
                    return

                file = datum['location'].split('/')[-1]

                hash = file.split('-')[-1]

                upload_path = os.path.join(self.DATADIR, file)

                # create a DID to upload
                did = make_did(number, dtype, hash)

                # check if a rule already exists for this DID on LNGS
                rucio_rule = self.rc.GetRule(upload_structure=did, rse="LNGS_USERDISK")
                helper.global_dictionary['logger'].Info('It was already in Rucio : {0}'.format(in_rucio))
                helper.global_dictionary['logger'].Info('Rucio rule : {0}'.format(rucio_rule))

                # if not in rucio already and no rule exists, upload into rucio
#                if not in_rucio and not rucio_rule['exists']:
                if not rucio_rule['exists']:
                    result = self.rc.Upload(did,
                                       upload_path,
                                       'LNGS_USERDISK',
                                       lifetime=None)
                    helper.global_dictionary['logger'].Info('Data type uploaded.')

                # if upload was successful, tell runDB
                rucio_rule = self.rc.GetRule(upload_structure=did, rse="LNGS_USERDISK")
                data_dict = {'host': "rucio-catalogue",
                             'type': dtype,
                             'location': 'LNGS_USERDISK',
                             'lifetime': rucio_rule['expires'],
                             'status': 'transferred',
                             'did': did,
                             'protocol': 'rucio'
                         }

#                if rucio_rule['state'] == 'OK':
#                    if not in_rucio:
#                        self.db.AddDatafield(run['_id'], data_dict)

                    # add a DID list that's easy to query by DB.GetDid
                    # check if did field exists yet or not
#                    if not run.get('dids'):
#                        self.db.db.find_one_and_update({'_id': run['_id']},
#                                                  {'$set': {'dids': {dtype: did}}}
#                        )
#                    else:
#                        helper.global_dictionary['logger'].Info('Updating DID list')
#                        self.db.db.find_one_and_update({'_id': run['_id']},
#                                                  {'$set': {'dids.%s' % dtype: did}}
#                        )

                # add rule to OSG and Nikhef
                # TODO make this configurable
#                for rse in ['UC_OSG_USERDISK', 'UC_DALI_USERDISK']:
#                for rse in ['UC_OSG_USERDISK', 'UC_DALI_USERDISK','CNAF_TAPE2_USERDISK','CNAF_USERDISK','NIKHEF2_USERDISK','CCIN2P3_USERDISK']:
#                for rse in ['UC_OSG_USERDISK', 'UC_DALI_USERDISK','NIKHEF2_USERDISK']:
                for rse in ['UC_DALI_USERDISK','NIKHEF2_USERDISK']:
                    self.add_rule(number, dtype, hash, rse)

                # finally, delete the eb copy
                #self.remove_from_eb(number, dtype)

#            if time.time() - last_check > self.periodic_check:
#                self.check_transfers()
#                last_check = time.time()


        return 0


    def __del__(self):
        pass
