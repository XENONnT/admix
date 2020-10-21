# -*- coding: utf-8 -*-
import json
import os
from admix.helper import helper
import time
import shutil
from datetime import timezone, datetime, timedelta

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

#get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater
from admix.utils import make_did
from admix.utils.list_file_replicas import list_file_replicas

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
        self.RUCIODATADIR = helper.get_hostconfig()['path_rucio_data']
        self.periodic_check = helper.get_hostconfig()['upload_periodic_check']
        self.RSES = helper.get_hostconfig()['rses']

        # Choose which RSE is used for the upload (usually it is LNGS_USERDISK)
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        self.minimum_number_acceptable_rses = 1
        self.minimum_deltadays_allowed = 0 #3
        self.minimum_deltadays_allowed_heavy = 0 #1
        self.dtype_delayed_delete = ['raw_records_aqmon','raw_records_aqmon_nv','raw_records_he','raw_records_mv','raw_records_nv','pulse_counts','pulse_counts_he','veto_regions','peaklets','peaklets_he','records_he']
        self.dtype_delayed_delete_heavy = ['raw_records','records']
        self.dtype_never_delete = ['lone_hits','merged_s2s','peak_basics','peaklet_classification']

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


        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES + self.NORECORDS_DTYPES
#        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES


        # Get all runs that are already transferred and that still have some data_types in eb 
        cursor = self.db.db.find({
#            'number': {"$lt": 7330, "$gte": 7300},
#            'number': {"$lt": 8570, "$gte": 8550},
#            'number': {"$gte": 7330},
            'number': {"$gte": 8500},
#            'number': 9226,
#            'data' : { "$elemMatch": { "host" : {"$regex" : ".*eb.*"} , "type" : {"$in" : data_types}} },
            'status': 'transferred'
        },
        {'_id': 1, 'number': 1, 'data': 1, 'bootstrax': 1})


        cursor = list(cursor)

        helper.global_dictionary['logger'].Info('Runs that will be processed are {0}'.format([c["number"] for c in cursor]))

        # Runs over all listed runs
        for run in cursor:

            #Gets the run number
            number = run['number']

            # Extracts the correct Event Builder machine who processed this run
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]

            helper.global_dictionary['logger'].Info('Treating run {0}'.format(number))

            helper.global_dictionary['logger'].Info('Run {0} has been processed by {1}'.format(number,eb))

            # Checks how much date are old
            run_time = run['bootstrax']['time'].replace(tzinfo=timezone.utc)
            now_time = datetime.now().replace(tzinfo=timezone.utc)
            delta_time = now_time - run_time

            # Loops on all datatypes that have to be cleaned
            for dtype in data_types:
                helper.global_dictionary['logger'].Info('\t==> Looking for data type {0}'.format(dtype))

                # checks the age of the data type
                is_enough_old = True

                # for some data types, it they are not yet older than three days, it skips deleting them
                if dtype in self.dtype_delayed_delete:
                    if delta_time < timedelta(days=self.minimum_deltadays_allowed):
                        helper.global_dictionary['logger'].Info('Run {0}, data type {1} is not yet older than {2} days. Skip it'.format(number,dtype,self.minimum_deltadays_allowed))
                        is_enough_old = False

                # for some heavy data types (records and raw_records), if they are not yet older than one day, it skips deleting them
                if dtype in self.dtype_delayed_delete_heavy:
                    if delta_time < timedelta(days=self.minimum_deltadays_allowed_heavy):
                        helper.global_dictionary['logger'].Info('Run {0}, data type {1} is not yet older than {2} days. Skip it'.format(number,dtype,self.minimum_deltadays_allowed_heavy))
                        is_enough_old = False

                # check first with runDB if the data type already exists in external RSEs
                rses_in_db = []
                for d in run['data']:
                    if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] != self.UPLOAD_TO:
                        rses_in_db.append(d['location'])
                helper.global_dictionary['logger'].Info('\t==> According to DB, found in following external RSEs : {0}'.format(rses_in_db))

                # if this is not the case, just skip any attempt of deleting anything
                if len(rses_in_db) < self.minimum_number_acceptable_rses:
                    helper.global_dictionary['logger'].Info('\t==> Nothing will be deleted : not enough external RSEs')
                    continue

                #
                # Phase 1 : Deleting data in EB
                #

                # check first if data are, according to the DB, still in EB
                datum = None
                for d in run['data']:
                    if d['type'] == dtype and eb in d['host']:
                        datum = d

                if datum is None:
                    helper.global_dictionary['logger'].Info('Data type not in eb')

                # start deleting data in EB
                if datum is not None and dtype not in self.dtype_never_delete and is_enough_old:
                    file = datum['location'].split('/')[-1]
                    hash = file.split('-')[-1]

                    # create the DID from DB
                    did = make_did(number, dtype, hash)

                    # check if a rule already exists with this exact DID in external RSEs
                    # and take also the number of files in each RSE
                    rses_with_rule = []
                    rses_with_correct_nfiles = []
                    for rse in self.RSES:
                        rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
                        if rucio_rule['exists'] and rucio_rule['state'] == 'OK':
                            if self.UPLOAD_TO==rucio_rule['rse']:
                                continue
                            rses_with_rule.append(rucio_rule['rse'])
                            nfiles = len(list_file_replicas(number, dtype, hash, rucio_rule['rse']))
                            if 'file_count' in datum:
                                if nfiles == datum['file_count']:
                                    rses_with_correct_nfiles.append(rucio_rule['rse'])
                    helper.global_dictionary['logger'].Info('\t==> According to Rucio, found in following external RSEs : {0}'.format(rses_with_rule))

                    if len(rses_with_correct_nfiles) == len(rses_with_rule):
                        helper.global_dictionary['logger'].Info('\t==> All of them with the expected number of files')
                    else:
                        helper.global_dictionary['logger'].Info('\t==> Error, these RSEs have wrong number of files : {0}'.format(rses_with_correct_nfiles))
                    
                    # if so, start deleting
#                    if len(rses_with_rule)>=self.minimum_number_acceptable_rses and len(rses_with_correct_nfiles) == len(rses_with_rule):
                    if len(rses_with_rule)>=self.minimum_number_acceptable_rses:

                        # delete from DB
                        # print(run['_id'],datum['type'],datum['host'])
                        self.db.RemoveDatafield(run['_id'],datum)
                        full_path = os.path.join(self.DATADIR, eb, file)
                        # print(full_path)

                        helper.global_dictionary['logger'].Info('\t==> Deleted EB info from DB') 

                        # delete from disk
                        try:
                            shutil.rmtree(full_path)
                        except OSError as e:
                            helper.global_dictionary['logger'].Info('\t==> Error, cannot delete directory : {0}'.format(e))
                        else:
                            helper.global_dictionary['logger'].Info('\t==> Deleted data from EB disk') 


                #
                # Phase 2 : Deleting data in LNGS_USERDISK
                #

                # check if data are, according to the DB, still in datamanager (LNGS_USERDISK)
                datum = None
                for d in run['data']:
                    if d['type'] == dtype and d['host'] == 'rucio-catalogue' and self.UPLOAD_TO in d['location']:
                        datum = d

                # if so, start deleting data in datamanager (LNGS_USERDISK)
                if datum is None:
                    helper.global_dictionary['logger'].Info('Data type not in LNGS_USERDISK')
                else:
                    # create the DID from DB
                    did = datum['did']
                    hash = did.split('-')[-1]

                    nfiles_upload_to = len(list_file_replicas(number, dtype, hash, self.UPLOAD_TO))

                    # check if a rule already exists with this exact DID in external RSEs
                    rses_with_rule = []
                    rses_with_correct_nfiles = []
                    for rse in self.RSES:
                        rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
                        if rucio_rule['exists'] and rucio_rule['state'] == 'OK':
                            if self.UPLOAD_TO==rucio_rule['rse']:
                                continue
                            rses_with_rule.append(rucio_rule['rse'])
                            nfiles = len(list_file_replicas(number, dtype, hash, rucio_rule['rse']))
                            if nfiles == nfiles_upload_to:
                                rses_with_correct_nfiles.append(rucio_rule['rse'])
                    helper.global_dictionary['logger'].Info('\t==> According to Rucio, found in following external RSEs : {0}'.format(rses_with_rule))

                    if len(rses_with_correct_nfiles) == len(rses_with_rule):
                        helper.global_dictionary['logger'].Info('\t==> All of them with the expected number of files')
                    else:
                        helper.global_dictionary['logger'].Info('\t==> Error, these RSEs have wrong number of files : {0}'.format(rses_with_correct_nfiles))

                    # if so, start deleting
                    if len(rses_with_rule)>=self.minimum_number_acceptable_rses and len(rses_with_correct_nfiles) == len(rses_with_rule):

                        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
                        if rucio_rule['exists'] and rucio_rule['state'] == 'OK' and rucio_rule['rse'] == self.UPLOAD_TO:
                            self.rc.DeleteRule(rucio_rule['id'])
                            helper.global_dictionary['logger'].Info('\t==> Deleted LNGS_USERDISK Rucio rule') 
                            hash = did.split('-')[-1]
                            files = list_file_replicas(number, dtype, hash, "LNGS_USERDISK")                        
                            for file in files:
                                os.remove(file)
                            helper.global_dictionary['logger'].Info('\t==> Deleted data from LNGS_USERDISK disk') 

                            self.db.RemoveDatafield(run['_id'],datum)
                            helper.global_dictionary['logger'].Info('\t==> Deleted LNGS_USERDISK info from DB') 
                    

        return 0


    def __del__(self):
        pass
