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

@Collector
class MonitorRun():

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

        # Storing some backup hashes in case DID information is not available
        self.bkp_hashes = { 'raw_records':'rfzvpzj4mf', 'raw_records_he':'rfzvpzj4mf', 'raw_records_mv':'rfzvpzj4mf', 'raw_records_aqmon':'rfzvpzj4mf', 'records':'56ausr64s7' }


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




    def run(self,*args, **kwargs):
        print('Run task '+self.__class__.__name__)

        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES + self.NORECORDS_DTYPES
#        data_types = self.RAW_RECORDS_DTYPES + self.RECORDS_DTYPES


        # Get all runs that are already transferred and that still have some data_types in eb 
        cursor = self.db.db.find({
#            'number': 7448
#            'number': 8322
#            'number': 6371
#            'number': 7148
#            'number': 8590
#            'number': 8626
            'number': 7148
#            'number': {'$gte': 8100, '$lt': 8200}
        },
        {'_id': 1, 'number': 1, 'data': 1, 'status':1, 'bootstrax': 1})

        cursor = list(cursor)

        print('Runs that will be processed are {0}'.format([c["number"] for c in cursor]))

        # Runs over all listed runs
        for run in cursor:

            # Gets run number
            number = run['number']
            print('Run: {0}'.format(number))

            # Extracts the correct Event Builder machine who processed this run                                                                               
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]
            print('Processed by: {0}'.format(eb))

            # Get the status
            if 'status' in run:
                print('Status: {0}'.format(run['status']))
            else:
                print('Status: {0}'.format('Not available'))

            # Merges data and deleted_data
            if 'deleted_data' in run:
                data = run['data'] + run['deleted_data']
            else:
                data = run['data']
            
            # Check is there are more instances in more EventBuilders
            extra_ebs = set()
            for d in data:
                if 'eb' in d['host'] and eb not in d['host']: 
                    extra_ebs.add(d['host'].split('.')[0])
            if len(extra_ebs)>0:
                print('\t\t Warning : The run has been processed by more than one EventBuilder: {0}'.format(extra_ebs))

            # Runs over all data types to be monitored
            for dtype in data_types:

                # Data type name
                print('{0}'.format(dtype))

                # Take the official number of files accordingto run DB
                Nfiles = -1
                for d in data:
                    if d['type'] == dtype and eb in d['host']:
                        if 'file_count' in d:
                            Nfiles = d['file_count']
                if Nfiles == -1:
                    print('\t Number of files: missing in DB')
                else:
                    print('\t Number of files: {0}'.format(Nfiles))

                # Check if data are still in the data list and not in deleted_data
                DB_InEB = False
                for d in run['data']:
                    if d['type'] == dtype and eb in d['host']:
                        DB_InEB = True
                DB_NotInEB = False
                if 'deleted_data' in run:
                    for d in run['deleted_data']:
                        if d['type'] == dtype and eb in d['host']:
                            DB_NotInEB = True
                if DB_InEB and not DB_NotInEB:
                    print('\t DB : still in EB')
                if not DB_InEB and DB_NotInEB:
                    print('\t DB : deleted from EB')
                if DB_InEB and DB_NotInEB:
                    print('\t\t Incoherency in DB: it is both in data list and in deleted_data list')
#                if (DB_InEB and DB_NotInEB) or (not DB_InEB and not DB_NotInEB):
#                    print('\t\t incoherency in DB: it is neither in data list nor in deleted_data list')

                # Check if data are still in the EB disks without using the DB
                upload_path = ""
                for d in run['data']:
                    if d['type'] == dtype and eb in d['host']:
                        file = d['location'].split('/')[-1]
                        upload_path = os.path.join(self.DATADIR, eb, file) 
                path_exists = os.path.exists(upload_path)
                if upload_path != "" and path_exists:
                    path, dirs, files = next(os.walk(upload_path))
                    print('\t Disk: still in EB disk and with',len(files),'files')
                else:
                    print('\t Disk: not in EB disk anymore')
                if DB_InEB and not path_exists:
                    print('\t\t Incoherency in DB and disk: it is in DB data list but it is not in the disk')
                if DB_NotInEB and path_exists:
                    print('\t\t Incoherency in DB and disk: it is in DB deleted_data list but it is still in the disk')

                # The list of DIDs (usually just one)
                dids = set()
                for d in data:
                    if d['type'] == dtype and d['host'] == 'rucio-catalogue':
                        if 'did' in d:
                            dids.add(d['did'])
                print('\t DID:', dids)

                # Check the presence in each available RSE
                Nrses = 0
                for rse in self.RSES:
                    is_in_rse = False
                    for d in run['data']:
                        if d['type'] == dtype and rse in d['location']:
                            if 'status' in d:
                                status = d['status']
                            else:
                                status = 'Not available'
                            if 'did' in d:
                                hash = d['did'].split('-')[-1]
                                did = d['did']
                            else:
                                print('\t\t Warning : DID information is absent in DB data list (old admix version). Using standard hashes for RSEs')
                                hash = self.bkp_hashes.get(dtype)
                                did = make_did(number, dtype, hash)
                            rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
                            files = list_file_replicas(number, dtype, hash, rse)
                            if rucio_rule['exists']:
                                print('\t', rse+': DB Yes, Status',status,', Rucio Yes, State',rucio_rule['state'],",",len(files), 'files')
                                if len(files) < Nfiles:
                                    print('\t\t Warning : Wrong number of files in Rucio!!!')
                            else:
                                print('\t', rse+': DB Yes, Status',status,', Rucio No')
                            # print(files)
                            is_in_rse = True
                            Nrses += 1
                    if not is_in_rse:
#                        print('\t\t Warning : data information is absent in DB data list. Trying using standard hashes to query Rucio')
                        hash = self.bkp_hashes.get(dtype)
                        did = make_did(number, dtype, hash)
                        rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
                        files = list_file_replicas(number, dtype, hash, rse)
                        if rucio_rule['exists']:
                            print('\t', rse+': DB No, Rucio Yes, State',rucio_rule['state'],",",len(files), 'files')
                            if len(files) < Nfiles:
                                print('\t\t Warning : Wrong number of files in Rucio!!!')
                        else:
                            print('\t', rse+': DB No, Rucio No')
                print('\t Number of sites: ', Nrses)
                    


        return 0


    def __del__(self):
        pass
