import os
from argparse import ArgumentParser
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.database import ConnectMongoDB
from admix.utils.naming import make_did,get_did
from admix.utils.list_file_replicas import list_file_replicas
from utilix.config import Config
import utilix
from bson.json_util import dumps
from datetime import timezone, datetime, timedelta
import pymongo
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient

class ShowRun():

    def __init__(self):

        #Take all data types categories
        self.RAW_RECORDS_TPC_TYPES = helper.get_hostconfig()['raw_records_tpc_types']
        self.RAW_RECORDS_MV_TYPES = helper.get_hostconfig()['raw_records_mv_types']
        self.RAW_RECORDS_NV_TYPES = helper.get_hostconfig()['raw_records_nv_types']
        self.LIGHT_RAW_RECORDS_TPC_TYPES = helper.get_hostconfig()['light_raw_records_tpc_types']
        self.LIGHT_RAW_RECORDS_MV_TYPES = helper.get_hostconfig()['light_raw_records_mv_types']
        self.LIGHT_RAW_RECORDS_NV_TYPES = helper.get_hostconfig()['light_raw_records_nv_types']
        self.HIGH_LEVEL_TYPES = helper.get_hostconfig()['high_level_types']
        self.RECORDS_TYPES = helper.get_hostconfig()['records_types']

        #Choose which data type you want to treat
        self.DTYPES = self.RAW_RECORDS_TPC_TYPES + self.RAW_RECORDS_MV_TYPES + self.RAW_RECORDS_NV_TYPES + self.LIGHT_RAW_RECORDS_TPC_TYPES + self.LIGHT_RAW_RECORDS_MV_TYPES + self.LIGHT_RAW_RECORDS_NV_TYPES + self.HIGH_LEVEL_TYPES + self.RECORDS_TYPES
        
        #Take the list of all XENON RSEs
        self.RSES = helper.get_hostconfig()['rses']

        #Take the RSE that is used to perform the upload
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        #Take the directory where datamanager has to upload data
        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']

        # Get the sequence of rules to be created according to the data type
        self.RAW_RECORDS_TPC_RSES = helper.get_hostconfig()["raw_records_tpc_rses"]
        self.RAW_RECORDS_MV_RSES = helper.get_hostconfig()["raw_records_mv_rses"]
        self.RAW_RECORDS_NV_RSES = helper.get_hostconfig()["raw_records_nv_rses"]
        self.LIGHT_RAW_RECORDS_TPC_RSES = helper.get_hostconfig()["light_raw_records_tpc_rses"]
        self.LIGHT_RAW_RECORDS_MV_RSES = helper.get_hostconfig()["light_raw_records_mv_rses"]
        self.LIGHT_RAW_RECORDS_NV_RSES = helper.get_hostconfig()["light_raw_records_nv_rses"]
        self.HIGH_LEVEL_RSES = helper.get_hostconfig()["high_level_rses"]
        self.RECORDS_RSES = helper.get_hostconfig()["records_rses"]

        #Init the runDB
        self.db = ConnectMongoDB()

        #Init Rucio for later uploads and handling:
        self.rc = RucioSummoner()
        self.didclient = DIDClient()
        self.replicaclient = ReplicaClient()

        #Rucio Rule assignment priority
        self.priority = 3

        #Parameters to write warnings
        self.minimum_number_acceptable_rses = 2
        self.minimum_deltadays_allowed = 3


    def showrun(self,arg_number,arg_to,arg_dtypes,arg_compact,arg_dumpjson,arg_status,arg_latest,arg_pending):

        #Define data types
        RAW_RECORDS_TPC_TYPES = helper.get_hostconfig()['raw_records_tpc_types']
        RAW_RECORDS_MV_TYPES = helper.get_hostconfig()['raw_records_mv_types']
        RAW_RECORDS_NV_TYPES = helper.get_hostconfig()['raw_records_nv_types']
        LIGHT_RAW_RECORDS_TPC_TYPES = helper.get_hostconfig()['light_raw_records_tpc_types']
        LIGHT_RAW_RECORDS_MV_TYPES = helper.get_hostconfig()['light_raw_records_mv_types']
        LIGHT_RAW_RECORDS_NV_TYPES = helper.get_hostconfig()['light_raw_records_nv_types']
        HIGH_LEVEL_TYPES = helper.get_hostconfig()['high_level_types']
        RECORDS_TYPES = helper.get_hostconfig()['records_types']

        #Get other parameters
        DATADIR = helper.get_hostconfig()['path_data_to_upload']
        RSES = helper.get_hostconfig()['rses']

        minimum_number_acceptable_rses = 2
        minimum_deltadays_allowed = 3

        # Storing some backup hashes in case DID information is not available
        bkp_hashes = { 'raw_records':'rfzvpzj4mf', 'raw_records_he':'rfzvpzj4mf', 'raw_records_mv':'rfzvpzj4mf', 'raw_records_aqmon':'rfzvpzj4mf', 'records':'56ausr64s7', 'lone_hits':'b7dgmtzaef' }

        context = 'xenonnt_online'

        #Init the runDB
        db = ConnectMongoDB()

        #Init Rucio for later uploads and handling:
        rc = RucioSummoner(helper.get_hostconfig("rucio_backend"))
        rc.SetRucioAccount(helper.get_hostconfig('rucio_account'))
        rc.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        rc.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        rc.SetHost(helper.get_hostconfig('host'))
        rc.ConfigHost()
        rc.SetProxyTicket("rucio_x509")


        data_types = RAW_RECORDS_TPC_TYPES + RAW_RECORDS_MV_TYPES + RAW_RECORDS_NV_TYPES + LIGHT_RAW_RECORDS_TPC_TYPES + LIGHT_RAW_RECORDS_MV_TYPES + LIGHT_RAW_RECORDS_NV_TYPES + HIGH_LEVEL_TYPES + RECORDS_TYPES


        # if arg_number has been given
        if arg_number != "":

            # if the "number" argument is a number, it is converted as integer
            if arg_number.isdigit():
                arg_number = int(arg_number)
            # otherwise it is assumed that a DID has been given and run number and other parameters are extracted from the DID
            else:
                arg_number, dtype, hash = get_did(arg_number)
                arg_dtypes = [dtype]

        # if no arg_number has been given, then the "latest" option is activated (with 5 run numbers by default) in compact modality
        else:
            if arg_latest == 0:
                arg_latest = 5
                arg_compact = True

        if arg_latest > 0:
            cursor = db.db.find({}).sort('number',pymongo.DESCENDING).limit(1)
            cursor = list(cursor)
            arg_to = cursor[0]['number']
            arg_number = arg_to - arg_latest + 1
            print('Processing latest {0} runs'.format(arg_latest))

        if arg_to>arg_number:
            cursor = db.db.find({'number': {'$gte': arg_number, '$lte': arg_to}}).sort('number',pymongo.ASCENDING)
            print('Runs that will be processed are from {0} to {1}'.format(arg_number,arg_to))
        else:
            cursor = db.db.find({'number': arg_number})

        print('Run that will be processed is {0}'.format(arg_number))
        cursor = list(cursor)

        # Runs over all listed runs
        for run in cursor:

            print("")

            # Gets run number
            number = run['number']
            print('Run: {0}'.format(number))

            # Gets the status
            if 'status' in run:
                print('Status: {0}'.format(run['status']))
            else:
                print('Status: {0}'.format('Not available'))

            if arg_status:
                continue

            # Extracts the correct Event Builder machine who processed this run
            # Then also the bootstrax state and, in case it was abandoned, the reason
            if 'bootstrax' in run:
                bootstrax = run['bootstrax']
                eb = bootstrax['host'].split('.')[0]
                print('Processed by: {0}'.format(eb))
                if 'state' in bootstrax:
                    print('Bootstrax state: {0}'.format(bootstrax['state']))
                    if bootstrax['state'] == 'abandoned':
                        if 'reason' in bootstrax:
                            print('Reason: {0}'.format(bootstrax['reason']))
            else:
                print('Not processed')

            # Gets the date
            if 'start' in run:
                start_time = run['start'].replace(tzinfo=timezone.utc)
                print("Date: ",start_time.astimezone(tz=None))

                # Calculates the duration
                if 'end' in run:
                    if run['end'] is not None:
                        end_time = run['end'].replace(tzinfo=timezone.utc)
                        duration = end_time-start_time
                        print("Duration: ",duration)
                    else:
                        print("Duration: ","unknown")

                # Prints if run is still enough recent (three days from now)
                now_time = datetime.now().replace(tzinfo=timezone.utc)
                delta_time = now_time-start_time
                if delta_time < timedelta(days=minimum_deltadays_allowed):
                    print("Less than {0} days old".format(minimum_deltadays_allowed))
            else:
                print("Warning : no time info available")


            # Gets the comments
            if 'comments' in run:
                if len(run['comments'])>0:
                    last_comment = run['comments'][-1]
                    print("Latest comment ({0}): {1}".format(last_comment['user'],last_comment['comment']))


            # Dumps the entire rundoc under json format
            if arg_dumpjson:
                print(dumps(run, indent=4))

            if arg_compact:
                continue

            # Merges data and deleted_data
    #        if 'deleted_data' in run:
    #            data = run['data'] + run['deleted_data']
    #        else:
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

                if len(arg_dtypes)>0:
                    if dtype not in arg_dtypes:
                        continue

                # Take the official number of files accordingto run DB
                # and the eb status
                Nfiles = -1
                ebstatus = ""
                for d in data:
                    if d['type'] == dtype and eb in d['host']:
                        if 'file_count' in d:
                            Nfiles = d['file_count']
                        if 'status' in d:
                            ebstatus = d['status']

                if arg_pending:
                    if ebstatus in ["","transferred"]:
                        continue

                # Data type name
                print('{0}'.format(dtype))

                if Nfiles == -1:
                    print('\t Number of files: missing in DB')
                else:
                    print('\t Number of files: {0}'.format(Nfiles))

                if ebstatus != "":
                    print('\t EB status: {0}'.format(ebstatus))
                else:
                    print('\t EB status: not available')


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
                #if (DB_InEB and DB_NotInEB) or (not DB_InEB and not DB_NotInEB):
                #  print('\t\t incoherency in DB: it is neither in data list nor in deleted_data list')

                # Check if data are still in the EB disks without using the DB
                upload_path = ""
                for d in run['data']:
                    if d['type'] == dtype and eb in d['host']:
                        file = d['location'].split('/')[-1]
                        upload_path = os.path.join(DATADIR, eb, file) 
                path_exists = os.path.exists(upload_path)
                if upload_path != "" and path_exists:
                    path, dirs, files = next(os.walk(upload_path))
                    print('\t Disk: still in EB disk and with',len(files),'files')
                else:
                    print('\t Disk: not in EB disk')
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
                for rse in RSES:
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
                                #hash = bkp_hashes.get(dtype)
                                #hash = utilix.db.get_hash(context, dtype)
                                hash = db.GetHashByContext(context,dtype)
                                did = make_did(number, dtype, hash)
                            rucio_rule = rc.GetRule(upload_structure=did, rse=rse)
                            files = list_file_replicas(number, dtype, hash, rse)
                            if rucio_rule['exists']:
                                print('\t', rse+': DB Yes, Status',status,', Rucio Yes, State',rucio_rule['state'],",",len(files), 'files')
                                if len(files) < Nfiles and rucio_rule['state']!="REPLICATING":
                                    print('\t\t Warning : Wrong number of files in Rucio!!!')
                            else:
                                print('\t', rse+': DB Yes, Status',status,', Rucio No')
                            # print(files)
                            is_in_rse = True
                            Nrses += 1
                    if not is_in_rse:
    #                    print('\t\t Warning : data information is absent in DB data list. Trying using standard hashes to query Rucio')
    #                    hash = bkp_hashes.get(dtype)
                        #hash = utilix.db.get_hash(context, dtype)
                        hash = db.GetHashByContext(context,dtype)
                        did = make_did(number, dtype, hash)
                        print('\t Guessed DID:', did)
                        rucio_rule = rc.GetRule(upload_structure=did, rse=rse)
                        files = list_file_replicas(number, dtype, hash, rse)
                        if rucio_rule['exists']:
                            print('\t', rse+': DB No, Rucio Yes, State',rucio_rule['state'],",",len(files), 'files')
                            if len(files) < Nfiles and rucio_rule['state']!="REPLICATING":
                                print('\t\t Warning : Wrong number of files in Rucio!!!')
                        else:
                            print('\t', rse+': DB No, Rucio No')
                print('\t Number of sites: ', Nrses)


    def showrun_new(self,arg_number,arg_to,arg_dtypes,arg_compact,arg_dumpjson,arg_status,arg_latest,arg_pending):

        # if arg_number has been given
        if arg_number != "":

            # if the "number" argument is a number, it is converted as integer
            if arg_number.isdigit():
                arg_number = int(arg_number)
            # otherwise it is assumed that a DID has been given and run number and other parameters are extracted from the DID
            else:
                arg_number, dtype, hash = get_did(arg_number)
                arg_dtypes = [dtype]

        # if no arg_number has been given, then the "latest" option is activated (with 5 run numbers by default) in compact modality
        else:
            if arg_latest == 0:
                arg_latest = 5
                arg_compact = True

        if arg_latest > 0:
            cursor = self.db.db.find({}).sort('number',pymongo.DESCENDING).limit(1)
            cursor = list(cursor)
            arg_to = cursor[0]['number']
            arg_number = arg_to - arg_latest + 1
            print('Processing latest {0} runs'.format(arg_latest))

        if arg_to>arg_number:
            cursor = self.db.db.find({'number': {'$gte': arg_number, '$lte': arg_to}}).sort('number',pymongo.ASCENDING)
            print('Runs that will be processed are from {0} to {1}'.format(arg_number,arg_to))
        else:
            cursor = self.db.db.find({'number': arg_number})
            print('Run that will be processed is {0}'.format(arg_number))

        cursor = list(cursor)

        # Runs over all listed runs
        for run in cursor:

            print("")

            # Gets run number
            number = run['number']
            print('Run: {0}'.format(number))

            # Gets the status
            if 'status' in run:
                print('Status: {0}'.format(run['status']))
            else:
                print('Status: {0}'.format('Not available'))

            if arg_status:
                continue

            # Extracts the correct Event Builder machine who processed this run
            # Then also the bootstrax state and, in case it was abandoned, the reason
            if 'bootstrax' in run:
                bootstrax = run['bootstrax']
                eb = bootstrax['host'].split('.')[0]
                print('Processed by: {0}'.format(eb))
                if 'state' in bootstrax:
                    print('Bootstrax state: {0}'.format(bootstrax['state']))
                    if bootstrax['state'] == 'abandoned':
                        if 'reason' in bootstrax:
                            print('Reason: {0}'.format(bootstrax['reason']))
            else:
                print('Not processed by EB')

            # Gets the date
            if 'start' in run:
                start_time = run['start'].replace(tzinfo=timezone.utc)
                print("Date: ",start_time.astimezone(tz=None))

                # Calculates the duration
                if 'end' in run:
                    if run['end'] is not None:
                        end_time = run['end'].replace(tzinfo=timezone.utc)
                        duration = end_time-start_time
                        print("Duration: ",duration)
                    else:
                        print("Duration: ","unknown")

                # Prints if run is still enough recent (three days from now)
                now_time = datetime.now().replace(tzinfo=timezone.utc)
                delta_time = now_time-start_time
                if delta_time < timedelta(days=self.minimum_deltadays_allowed):
                    print("Less than {0} days old".format(self.minimum_deltadays_allowed))
            else:
                print("Warning : no time info available")


            # Gets the comments
            if 'comments' in run:
                if len(run['comments'])>0:
                    last_comment = run['comments'][-1]
                    print("Latest comment ({0}): {1}".format(last_comment['user'],last_comment['comment']))


            # Dumps the entire rundoc under json format
            if arg_dumpjson:
                print(dumps(run, indent=4))

            if arg_compact:
                continue

            # Runs over all data types stored in data and deleted_data fields
            alldata = run['data']
            if 'deleted_data' in run:
                alldata = alldata + run['deleted_data']

            for datum in alldata:

                if arg_pending:
                    ebstatus = ""
                    if 'status' in datum:
                        ebstatus = datum['status']
                    if ebstatus in ["","transferred"]:
                        continue

                if len(arg_dtypes)>0:
                    if datum['type'] not in arg_dtypes:
                        continue

                if eb in datum['host']:
                    self.showdataset(run,datum)


    def showdataset(self,run,datum):


        #print(dumps(datum, indent=4))

        # skip dataset if it does not have location
        if 'location' not in datum:
            print('Dataset: type {0} without location. Skipping'.format(datum['type']))
            return

        # Dataset name
        number = run['number']
        dtype = datum['type']
        hash = datum['location'].split('-')[-1]
        did = make_did(number, dtype, hash)
        print('Dataset: {0}'.format(did))

        # Event builder who treated it
        eb = datum['host'].split('.')[0]

        # Directory name
        directory = datum['location'].split('/')[-1]

        # Take the official number of files according to run DB
        Nfiles = -1
        if 'file_count' in datum:
            Nfiles = datum['file_count']
        if Nfiles == -1:
            print('\t Number of files: missing in DB')
        else:
            print('\t Number of files: {0}'.format(Nfiles))

        # Take the status of the EB dataset according to run DB
        ebstatus = ""
        if 'status' in datum:
            ebstatus = datum['status']
        if ebstatus != "":
            print('\t EB status: {0}'.format(ebstatus))
        else:
            print('\t EB status: not available')

        # Check if there are double entries in the DB
        Copies = 0
        for d in run['data']:
            if d['type'] == dtype and eb in d['host'] and hash in d['location']:
                Copies = Copies + 1
        if Copies>1:
            print('\t\t Warning {0}: EB datum has a double entry in the DB'.format(did))

        # Check if there are other entries in the deleted_data (even with different EBs)
        #DeletedCopies = []
        #for d in run['deleted_data']:
        #    if d['type'] == dtype and hash in d['location']:
        #        DeletedCopies.append(d['host'].split('.')[0])
        #if len(DeletedCopies)>0:
        #    print('\t Previously deleted data processed with those EBs: {0}'.format(DeletedCopies))

        # Read the real number of files present in EB disks
        upload_path = os.path.join(self.DATADIR, eb, directory) 
        path_exists = os.path.exists(upload_path)
        Nfiles_disk = 0
        if path_exists:
            path, dirs, files = next(os.walk(upload_path))
            Nfiles_disk = len(files)

        # If data are supposed to be (according to DB) still present in EB, check if they are there
        if datum in run['data']:
            print('\t Still in EB')
            if Nfiles_disk != Nfiles:
                print('\t\t Warning {0}: number of files in EB disk ({1}) does not match with the DB info ({2})'.format(did,Nfiles_disk,Nfiles))

        # Otherwise, if data are supposed to be (according to DB) deleted, check if they are really absent
        elif datum in run['deleted_data']:
            print('\t Deleted from EB')
            if Nfiles_disk > 0:
                print('\t\t Warning {0}: files are still in EB disk (nfiles={1}) while DB says they are deleted '.format(did,Nfiles_disk))

        
        # Query rucio to see how many RSEs have those data
        rules = list(self.didclient.list_did_rules(did.split(':')[0], did.split(':')[1]))
        rses_with_data = []
        for rule in rules:
            rses_with_data.append(rule['rse_expression'])
        if len(rses_with_data)>0:
            print('\t Rucio replicas in {0} RSEs : {1}'.format(len(rses_with_data),rses_with_data))
        else:
            print('\t No replicas in Rucio')
            
        # Check the presence of data in each available RSE and compatibility with DB

        # Step 1: prepare the dictionary
        rses = []
        for rse in self.RSES:
            r = {}
            r['name'] = rse
            rses.append(r)

        Nrses = 0

        # Step 2: filling the dictionary with RSEs info from DB and from Rucio
        for rse in rses:
            is_in_rse = False

            # Get info available in the DB
            rse['DBentries'] = 0
            rse['DBStatus'] = ""
            for d in run['data']:
                if 'rucio' in d['host']:
                    if d['did'] == did and rse['name'] in d['location']:
                        if 'status' in d:
                            rse['DBStatus'] = d['status']
                        rse['DBentries'] = rse['DBentries'] + 1
                    
            # Get info available in Rucio
            rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse['name'])
            #            files = list_file_replicas(number, dtype, hash, rse['name'])
            #            files = list(self.rc.ListFileReplicas(did,rse['name'],localpath=True).values())
            did_dictionary = [{'scope' : did.split(':')[0], 'name' : did.split(':')[1]}]
            replicas = list(self.replicaclient.list_replicas(did_dictionary,rse_expression=rse['name']))
            #print(dumps(replicas, indent=4))
            rse['RucioExists'] = rucio_rule['exists']
            rse['RucioNFiles'] = len(replicas)


        # Step 3: analysis of data
        for rse in rses:

            #print(rse)

            # analysis specific for uploading
            if rse['name']==self.UPLOAD_TO:

                # Case 1 : loss of Rucio connection at the end of the upload before creating the rule
                if rse['RucioNFiles']==Nfiles and not rse['RucioExists'] and rse['DBStatus']=="" and rse['DBentries']==0 and len(rses_with_data)==0:
                    print('\t\t Warning: files have been uploaded but the rule has not been created')
                    print('\t\t Hint: create the rule manually, then continue uploading, using the following three commands:')
                    print('\t\t\t rucio add-rule {0} 1 {1}'.format(did,rse['name']))
                    print('\t\t\t admix-fix --fix_upload_db {0}'.format(did))
                    print('\t\t\t admix-fix --create_upload_rules {0}'.format(did))
#                    os.system('rucio add-rule {0} 1 {1}'.format(did,rse['name']))
#                    os.system('~/.local/bin/admix-fix --fix_upload_db {0}'.format(did))
#                    os.system('~/.local/bin/admix-fix --create_upload_rules {0}'.format(did))

                # Case 2 : loss of Rucio connection at the end of the upload before updating the DB
                if rse['RucioNFiles']==Nfiles and rse['RucioExists'] and rse['DBStatus']=="" and rse['DBentries']==0 and len(rses_with_data)==1:
                    print('\t\t Warning: the upload is completed, but DB needs to be updated and rules have to be created abroad')
                    print('\t\t Hint: fix it manually with the two commands:')
                    print('\t\t\t admix-fix --fix_upload_db {0}'.format(did))
                    print('\t\t\t admix-fix --create_upload_rules {0}'.format(did))
#                    os.system('~/.local/bin/admix-fix --fix_upload_db {0}'.format(did))
#                    os.system('~/.local/bin/admix-fix --create_upload_rules {0}'.format(did))

                # Case 3 : loss of Rucio connection at the end of the upload before creating the rules abroad
                if rse['RucioNFiles']==Nfiles and rse['RucioExists'] and rse['DBStatus']=="transferred" and rse['DBentries']==1 and len(rses_with_data)==1:
                    print('\t\t Warning: the upload is completed and the DB updated, but rules have to be created abroad')
                    print('\t\t Hint: fix it manually with the command:')
                    print('\t\t\t admix-fix --create_upload_rules {0}'.format(did))
#                    os.system('~/.local/bin/admix-fix --create_upload_rules {0}'.format(did))

                # Case 4 : data still to be uploaded but the value if the EB status is not empty so admix cannot upload it
                if rse['RucioNFiles']==0 and not rse['RucioExists'] and rse['DBStatus']=="" and rse['DBentries']==0 and len(rses_with_data)==0 and ebstatus not in ["","transferred"]:
                    print('\t\t Warning: the upload never started but the EB status is not empty, hence admix cannot upload it')
                    print('\t\t Hint: fix it manually with the following command to allow admix upload manager to take care of it:')
                    print('\t\t\t admix-fix --set_eb_status {0} eb_ready_to_upload'.format(did))
#                    os.system('~/.local/bin/admix-fix --set_eb_status {0} eb_ready_to_upload'.format(did))

                # Case 4 : data still to be uploaded but the value if the EB status is not empty so admix cannot upload it
                if rse['RucioNFiles']==Nfiles and rse['RucioExists'] and rse['DBStatus']=="transferred" and rse['DBentries']==1 and len(rses_with_data)>0 and ebstatus not in ["","transferred"]:
                    print('\t\t Warning: the upload is completed and there are also copies abroad')
                    print('\t\t Hint: fix it manually with the command below to flag the EB datum as transferred:')
                    print('\t\t\t admix-fix --set_eb_status {0} transferred'.format(did))
#                    os.system('~/.local/bin/admix-fix --set_eb_status {0} transferred'.format(did))

                # Case 5 : data still to be uploaded but the value if the EB status is not empty so admix cannot upload it
                if rse['RucioNFiles']!=Nfiles and rse['RucioExists'] and rse['DBStatus']=="" and rse['DBentries']==0 and len(rses_with_data)==1 and ebstatus=="transferring":
                    print('\t\t Warning: the upload has been interrupted during the copy')
                    print('\t\t Hint: fix it manually with the command below to resume the upload:')
                    print('\t\t\t admix-fix --fix_upload {0}'.format(did))
            
            # analysis for all RSEs other than datamanager
            else:

                if not (
                        (rse['RucioNFiles']==Nfiles and rse['RucioExists'] and rse['DBentries']==1 and rse['DBStatus']=='transferred')
                        or (rse['RucioNFiles']==0 and not rse['RucioExists'] and rse['DBentries']==0 and rse['DBStatus']!='transferred')
                        ):
                    print('\t\t Warning {0}: data in RSE {1} are inconsistent:'.format(did,rse['name']))
                    print('\t\t ',rse)



#        print(dumps(rses, indent=4))


    

def main():
    parser = ArgumentParser("admix-showrun")

    config = Config()

    parser.add_argument("number", nargs='?', help="Run number or DID to show", default="")
    parser.add_argument("--dtypes", nargs="*", help="Restricts infos on the given data types")
    parser.add_argument("--to", type=int, help="Shows runs from the run number up to this value", default=0)
    parser.add_argument("--compact", help="Just list few DB infos as run number, status, date, comments", action='store_true')
    parser.add_argument("--status", help="Just list the run name and its global status", action='store_true')
    parser.add_argument("--pending", help="Shows only pending data types", action='store_true')
    parser.add_argument("--json", help="Dumps the whole DB rundoc in pretty style", action='store_true')
    parser.add_argument("--latest", type=int, help="Shows latest runs", default=0)

    parser.add_argument("--new", help="New version under development", action='store_true')

    args = parser.parse_args()

    if args.dtypes:
        dtypes = args.dtypes
    else:
        dtypes = []

    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))

    showrun = ShowRun()

    try:
        if args.new:
            showrun.showrun_new(args.number,args.to,dtypes,args.compact,args.json,args.status,args.latest,args.pending)
        else:
            showrun.showrun(args.number,args.to,dtypes,args.compact,args.json,args.status,args.latest,args.pending)
    except KeyboardInterrupt:
        return 0



