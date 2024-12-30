import os
import time
import shutil
import psutil
from argparse import ArgumentParser
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.database import ConnectMongoDB
from admix.utils.naming import make_did
from admix.utils.list_file_replicas import list_file_replicas
from rucio.client.replicaclient import ReplicaClient
from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient
from rucio.client.client import Client
from admix import download
from utilix.config import Config
import utilix
from bson.json_util import dumps
from datetime import timezone, datetime, timedelta
import pymongo
import glob
import tarfile
import gfal2
from tqdm import tqdm
from admix.utils import make_did

from pymongo import ReturnDocument

class Fix():

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

        #Init the Rucio replica client
        self.replicaclient = ReplicaClient()

        #Init the Rucio upload client
        self.upload_client = UploadClient()

        #Init the Rucio upload client
        self.did_client = DIDClient()

        #Init the Rucio client
        self.rucio_client = Client()

        #Rucio Rule assignment priority
        self.priority = 3

        self.working_path='/home/XENON/scotto/scratch'

    def tarball_all(self,from_rse,to_rse):

        print("Tarballing all rules from {0} to {1}".format(from_rse,to_rse))

        dsns = list(set(['%s:%s' % (dsn['scope'], dsn['name']) for dsn in self.rucio_client.list_datasets_per_rse(from_rse)]))
        dsns.sort()
        print("SCOPE:NAME")
        print('----------')
        start = False
        for dsn in tqdm(dsns):
            if 'xnt' not in dsn:
                continue
            if 'tarball' in dsn:
                continue

            if dsn == "xnt_010490:raw_records_aqmon-rfzvpzj4mf":
                start = True

            if start:
                print("Start tarballing of {0}".format(dsn))
                if not self.tarball(dsn,from_rse,to_rse):
                    break
#                break

#            if dsn == "xnt_025860:raw_records_nv-rfzvpzj4mf":
#                break



    def tarball(self,did,from_rse,to_rse):

        # check from the name if the rule is already tarballed
        tar_suffix = did.split('.')[-1]
        if tar_suffix == "tarball":
            print("Error! According to the name, the rule {0} is already tarballed".format(did))
            return(True)

        # define relevant names
        scope, dataset = did.split(':')
        dataset_tar = dataset+'.tarball'
        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Tarballing rule {0} from {1} to {2}".format(did,from_rse,to_rse))
#        print("Run number: {0}".format(number))
#        print("Data type: {0}".format(dtype))
#        print("Hash: {0}".format(hash))

        # tarball only specific data types
#        if dtype not in ["raw_records","raw_records_mv","raw_records_nv","raw_records_aqmon"]:
#            print("Error! This data type cannot be tarballed because usually it has very few files")
#            return True

        # check if the tarball rule already exists in any rse
        tarballrule_exists_already = False
        rules = list(self.rucio_client.list_did_rules(scope, dataset_tar))
        if len(rules)>0:
            print("Error! The tarball version of this did exists already. I cannot start another tarballing")
            return True
            

        # check if the rule exists and it is present in from_rse
        rule_is_ok = False
        nfiles = 0
        rules = list(self.rucio_client.list_did_rules(scope, dataset))
        rule_id = 0
        for rule in rules:
            if rule['rse_expression']==from_rse:
#                print(rule['rse_expression'],rule['state'],rule['id'],rule['locks_ok_cnt'])
                if rule['state']=='OK':
                    rule_is_ok = True
                    nfiles = rule['locks_ok_cnt']
                    rule_id = rule['id']
        if not rule_is_ok:
            print("Error! Rule is not OK. Tarballing cannot be done")
            return True

        if nfiles==0:
            print("Error! Rule has no files. Tarballing cannot be done")
            return True

        if nfiles==1:
            print("Warning! Rule is OK but there is only the metadata. Tarballing cannot be done")
            return True

        if nfiles==2:
            print("Warning! Rule is OK but there is only one chunk. Tarballing will not be done")
            return True
 
        print("Rule is OK, the number of files is {0}, its ID is {1}".format(nfiles,rule_id))

#        return

        # prestage rule
        self.bring_online(did,from_rse)

        # download rule
        download(number,dtype,hash,rse=from_rse,location=self.working_path,tries=10)

        # create the directory that will contain the new dataset
        filename = did.replace(':', '-')
        filename = filename.replace('xnt_', '')
        path_to_tar = os.path.join(self.working_path,filename)
        path_to_upload = path_to_tar+'.tar'
        os.mkdir(path_to_upload)

        # check if the number of downloaded files corresponds to the ones declared by Rucio
        downloaded_files = os.listdir(path_to_tar)
        #print(len(downloaded_files),nfiles)
        if len(downloaded_files)!=nfiles:
            print("Error! The number of downloaded files does not correspond to the one declared by Rucio. Tarballing is stop")            
            return False

        # tarball the rule excluding the metadata
        tar_filename = os.path.join(path_to_upload,dataset+'.tar')
        print("Creating tarball {0}".format(tar_filename))
        tar_file = tarfile.open(tar_filename,"w")
        for filetoadd in sorted(downloaded_files):
            filetoadd_with_path = os.path.join(path_to_tar, filetoadd)
            if "metadata" in filetoadd:
                continue
            tar_file.add(filetoadd_with_path,arcname=filetoadd)
        tar_file.close()

        # (deprecated, we don't need to copy the metadata to upload it, we just need to attach the one already existing in Rucio) copy the metadata in the path to upload
###        metadata_filename = os.path.join(self.working_path,filename,dataset+'-metadata.json')
###        shutil.copy(metadata_filename, path_to_upload)

        # remove the downloaded directory
        print("Removing directory {0}".format(path_to_tar))
        shutil.rmtree(path_to_tar)

        # don't uncomment this line. creating the scope should not needed in normal situations since the scope already exists
        # self.rucio_client.add_scope(account='production', scope=scope)

        # upload rule
        print("Uploading the rule {0}".format(did))
        upload_dict = dict(path=path_to_upload,
                           rse=to_rse,
                           lifetime=None,
                           did_scope=scope,
                           dataset_scope=scope,
                           dataset_name=dataset_tar,
                           )
        self.upload_client.upload([upload_dict])

        # attach the already existing metadata to the new, tarball, did
        print("Attaching the already existing metadata to the rule {0}".format(did))
        metadata_did = []
        metadata_did.append(dict(scope=scope, name=dataset+'-metadata.json'))
        self.did_client.attach_dids(scope,dataset_tar,metadata_did,rse=to_rse)

        # don't uncomment this line. Not needed except for very exceptional cases
        #os.system("rucio add-rule "+scope+":"+dataset_tar+" 1 "+to_rse) # not needed

        # update database with a new data entry starting with the original did
        self.add_db_rule_tar(did,from_rse,to_rse)

        # remove from the RSE and from database the non-tarball data entry
        print("Removing the rule {0} from {1} and from the database".format(did,from_rse))
        self.delete_rule(did,from_rse)

        # remove the downloaded directory
        print("Removing the tar directory {0}".format(path_to_upload))
        shutil.rmtree(path_to_upload)

        return True



    def bring_online(self,did,rse):
        print("Bringing online {0} from {1}".format(did,rse))
        
        scope = did.split(':')[0]
        dataset = did.split(':')[1]

        file_replicas = Client().list_replicas([{'scope':scope,'name': dataset}],rse_expression=rse)
        files = [list(replica['pfns'].keys())[0] for replica in file_replicas]

        print("Bringing online {0} files".format(len(files)))

        if rse=="SURFSARA_USERDISK":
            for i, file in enumerate(files):
                files[i] = files[i].replace("gsiftp","srm")
                files[i] = files[i].replace("gridftp","srm")
                files[i] = files[i].replace("2811","8443")
        if rse=="CCIN2P3_USERDISK":
            for i, file in enumerate(files):
                files[i] = files[i].replace("gsiftp","srm")
                files[i] = files[i].replace("ccdcacli392.in2p3.fr","ccsrm02.in2p3.fr")
                files[i] = files[i].replace("2811","8443")
        #print(files)

        ctx = gfal2.creat_context()

        try:
            # bring_online(surls, pintime, timeout, async)
            # Parameters:
            #   surls is the given [srmlist] argument
            #   pintime in seconds (how long should the file stay PINNED), e.g. value 1209600 will pin files for two weeks
            #   timeout of request in seconds, e.g. value 604800 will timeout the requests after a week
            #   async is asynchronous request (does not block if != 0)
            pintime = 3600*48
            timeout = 3600
            (status, token) = ctx.bring_online(files, pintime, timeout, True)
            if token:
                print(("Got token %s" % token))
            else:
                print("No token was returned. Are all files online?")
        except gfal2.GError as e:
            print("Could not bring the files online:")
            print(("\t", e.message))
            print(("\t Code", e.code))

        if self.no_wait:
            return

        print("Waiting until they are all online... (this might take time)")
        while True:
            errors = ctx.bring_online_poll(files, token)
            ncompleted = 0
            for surl, error in zip(files, errors):
                if not error:
                    ncompleted += 1
            print("So far {0} files have been staged".format(ncompleted))
            if ncompleted == len(files):
                print("Staging of {0} files successfully completed".format(ncompleted))
                break
            time.sleep(60)








    def clean_empty_directories_rse(self,rse):
        print("Cleaning all empty directories present in {0}".format(rse))

        scopes = list(set([dsn['scope'] for dsn in self.rucio_client.list_datasets_per_rse(rse)]))
        scopes.sort()
        print("SCOPE:NAME")
        print('----------')
        for scope in scopes:
            if 'xnt' not in scope:
                continue
#            if scope!='xnt_007177':
#                continue
            print(scope)
            directory = os.path.join('srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/xenon/rucio/',scope)
            print(directory)
            number = int(scope.split('_')[-1])
            print(number)
            if number>49000:
                continue
            self.clean_empty_directories(directory)


    def clean_empty_directories(self,directory):
        print(directory)
        ctx = gfal2.creat_context()
        list_subdir_lev1 = ctx.listdir(directory)
        for subdir_lev1 in list_subdir_lev1:
            if subdir_lev1 == '.' or subdir_lev1 == '..':
                continue
            directory_lev1 = os.path.join(directory,subdir_lev1)
#            print(directory_lev1)
            list_subdir_lev2 = ctx.listdir(directory_lev1)
            for subdir_lev2 in list_subdir_lev2:
                if subdir_lev2 == '.' or subdir_lev2 == '..':
                    continue
                directory_lev2 = os.path.join(directory_lev1,subdir_lev2)
#                print(directory_lev2)
                list_subdir_lev3 = ctx.listdir(directory_lev2)
                if len(list_subdir_lev3)==0:
                    print("No files. Proceeding with deleting directory {0}".format(directory_lev2))
                    ctx.rmdir(directory_lev2)
                for subdir_lev3 in list_subdir_lev3:
                    if subdir_lev3 == '.' or subdir_lev3 == '..':
                        continue
                    files_lev3 = os.path.join(directory_lev2,subdir_lev3)
#                    print(files_lev3)
            list_subdir_lev2 = ctx.listdir(directory_lev1)
            if len(list_subdir_lev2)==0:
                print("No files. Proceeding with deleting directory {0}".format(directory_lev1))
                ctx.rmdir(directory_lev1)



    def reset_upload(self,did):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Resetting the upload associated to the DID: {0}".format(did))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number' : number})

        # Gets the status
        if 'status' in run:
            print('Run status: {0}'.format(run['status']))
        else:
            print('Run status: {0}'.format('Not available'))

        # Extracts the correct Event Builder machine who processed this run
        # Then also the bootstrax state and, in case it was abandoned, the reason
        if 'bootstrax' in run:
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]
        else:
            print('Not processed')
            return(0)
                

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        if datum is None:
            print('There is no EB datum. No reset is possible')
            return(0)

        if ebstatus != "":
            print('EB status: {0}'.format(ebstatus))
        else:
            print('EB status: not available')


        # Step zero (normally not needed): change the run status to "transferring"
        #    self.db.db.find_one_and_update({'number':number},{'$set':{"status": "transferring"}})


        # First action: remove the files stored in datamanager
        files = list_file_replicas(number, dtype, hash, self.UPLOAD_TO)
        print("Deleting rucio data in datamanager disk. Deleting",len(files),"files")
        for file in files:
            try:
                os.remove(file)
            except:
                print("File: {0} not found".format(file))



        # Second action: remove the LNGS Rucio rule
        deleted_any_rule = False
        for rse in self.RSES:
            rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)
            if rucio_rule['exists']:
                print("Deleting rucio rule = ", rucio_rule['id'], "from RSE = ",rse)
                self.rc.DeleteRule(rucio_rule['id'])
                deleted_any_rule = True


        # Third action: remove possible files in datamanager in case the Rucio rule does not exists
        datamanager_rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        if not datamanager_rucio_rule['exists']:
            print("Rucio rule not existing. Deleting data in datamanager without Rucio")
            filelistname = os.path.join("/archive/data/rucio/xnt_%06d/*/*/" % number, dtype+"-"+hash+"*")
            filelist = glob.glob(filelistname)
            for filePath in filelist:
                try:
                    os.remove(filePath)
                except:
                    print("Error while deleting file : ", filePath)



        # If some rule has been deleted, wait for 1 hour (plus 5 minutes of margin)
        if deleted_any_rule:
            print("We have to wait until the rule is fully deleted before changing the status of the datum. It could take at least an hour")
            while True:
                datamanager_rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
                if not datamanager_rucio_rule['exists']:
                    print("Rule for did {0} finally deleted".format(did))
                    break
                delay = 60*10
                time.sleep(delay)
        else:
            print("There is no rule to delete")



        # Fourth action: set the EB status as 'eb_ready_to_upload' 
        self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
                                       { '$set': { "data.$.status" : 'eb_ready_to_upload' } })

        print("EB status changed to eb_ready_to_upload")



        # Reload the run
        run = self.db.db.find_one({'number' : number})

        # Gets the status
        if 'status' in run:
            print('New run status: {0}'.format(run['status']))
        else:
            print('Ru status: {0}'.format('Not available'))

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        # Prints the eb status as a confirmation of the performed change 
        if ebstatus != "":
            print('New EB status: {0}'.format(ebstatus))
        else:
            print('New EB status: not available')



    def add_rule(self,did,from_rse,to_rse):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Adding a new rule {0} from {1} to {2}".format(did,from_rse,to_rse))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number' : number})

        # Gets the status
        if 'status' in run:
            print('Run status: {0}'.format(run['status']))
        else:
            print('Run status: {0}'.format('Not available'))

        #Checks if the datum of the sender exists in the DB
        datum = None
        for d in run['data']:
            if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == from_rse:
                datum = d
                break
        if datum is None:
            print('The datum concerning data type {0} and site {1} is missing in the DB'.format(dtype,from_rse))
            if not self.ignore_db_errors:
                print('Forced to stop')
                return(0)


        # Checks the rule status of the sender RSE
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=from_rse)
        if rucio_rule['state'] != 'OK' and rucio_rule['state'] != 'REPLICATING':
            print('The rule in {0} is neither OK nor REPLICATING. Forced to stop'.format(from_rse))
            return(0)

        # set the new rule
        if not self.skip_rucio:
            print("Adding the Rucio rule")
            self.rc.AddConditionalRule(did, from_rse, to_rse, lifetime=None, priority=self.priority)
        else:
            print("Rucio rule is not added")
        rucio_rule = self.rc.GetRule(did, rse=to_rse)

        if datum is not None:
            # Update run status
            self.db.db.find_one_and_update({'number': number},{'$set': {'status': 'transferring'}})

            # Add a new datum in the run document
            updated_fields = {'host': "rucio-catalogue",
                              'type': dtype,
                              'location': to_rse,
                              'lifetime': rucio_rule['expires'],
                              'status': 'transferring',
                              'did': did,
                              'protocol': 'rucio'
                          }
            data_dict = datum.copy()
            data_dict.update(updated_fields)
            self.db.AddDatafield(run['_id'], data_dict)

        print("Done.")


    def add_rules_from_file(self,filename,from_rse,to_rse):

        with open(filename) as f:
            dids = f.read().splitlines()
            f.close()

        for did in dids:

            if did[0] == "#":
                continue

            hash = did.split('-')[-1]
            dtype = did.split('-')[0].split(':')[-1]
            number = int(did.split(':')[0].split('_')[-1])

            timestamp = time.strftime("%Y-%m-%d-%H-%M-%S",time.localtime(time.time()))

            print("{0} - Adding a new rule {1} from {2} to {3}".format(timestamp,did,from_rse,to_rse))

            # Checks the rule status of the sender RSE
            rucio_rule = self.rc.GetRule(upload_structure=did, rse=from_rse)
            if rucio_rule['state'] != 'OK' and rucio_rule['state'] != 'REPLICATING':
                print('The rule in {0} is neither OK nor REPLICATING. Skipping this DID'.format(from_rse))
                continue

            # Checks the rule status of the destination RSE
            rucio_rule = self.rc.GetRule(upload_structure=did, rse=to_rse)
            if rucio_rule['exists']:
                print('The rule in {0} already exists and its status is {1}. Skipping this DID'.format(to_rse,rucio_rule['state']))
                continue

            # Creates the new rule
            print("Adding the Rucio rule")
            self.rc.AddConditionalRule(did, from_rse, to_rse, lifetime=None, priority=5)

            # Waits until Rucio sees this rule as successfully transferred
            print("Waiting until the transfer is completed")
            rule_is_ok = False
            while not rule_is_ok:
                delay = 10 #60
                time.sleep(delay)
                rucio_rule = self.rc.GetRule(did, rse=to_rse)
                if rucio_rule['state'] == 'OK':
                    rule_is_ok = True
            print("Transfer completed")

            wait_time = 10
            print('Waiting for {0} seconds'.format(wait_time))
            print("You can safely CTRL-C now if you need to stop me")
            try:
                time.sleep(wait_time)
            except KeyboardInterrupt:
                break



    def add_db_rule_tar(self,did,from_rse,to_rse):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])
        didtar = did+'.tarball'

        print("Adding a new DB datum associated to the did {0} in {1} using infos from {2}".format(didtar,to_rse,from_rse))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))
        print("Did tar: {0}".format(didtar))

        run = self.db.db.find_one({'number' : number})

        # Gets the status
        if 'status' in run:
            print('Run status: {0}'.format(run['status']))
        else:
            print('Run status: {0}'.format('Not available'))

        #Checks if the datum of the sender exists in the DB
        datum = None
        for d in run['data']:
            if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == from_rse:
                datum = d
                break
        if datum is None:
            print('The datum concerning data type {0} and site {1} is missing in the DB. Forced to stop'.format(dtype,from_rse))
            return(0)

        #Checks if the datum of the destination does not exist yet in the DB
        datumtar = None
        for d in run['data']:
            if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == to_rse:
                if 'did' in d:
                    if d['did']==didtar:
                        datumtar = d
                        break
        if datumtar is not None:
            print('The datum concerning data type {0} and site {1} exists already in the DB. Forced to stop'.format(dtype,to_rse))
            return(0)


        # Checks the rule status of the destination RSE
        rucio_rule = self.rc.GetRule(upload_structure=didtar, rse=to_rse)
        if not rucio_rule['exists']:
            print('The rule {0} in {1} does not exist. Forced to stop'.format(didtar,to_rse))
            return(0)

        if rucio_rule['state'] != 'OK':
            print('The rule {0} in {1} exists but it is not OK. Forced to stop'.format(didtar,to_rse))
            return(0)

        # Add a new data field copying from from_rse but with: to_rse as RSE, dtypetar as dtype and with status "trasferred"
        data_dict = datum.copy()
        data_dict.update({'host': "rucio-catalogue",
                          'type': dtype,
                          'location': to_rse,
                          'lifetime': rucio_rule['expires'],
                          'status': 'transferred',
                          'did': didtar,
                          'protocol': 'rucio'
                      })
#        print(data_dict)
        self.db.AddDatafield(run['_id'], data_dict)

        print("Done.")




    def delete_rule(self,did,rse):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Deleting the rule {0} from {1}".format(did,rse))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number' : number})

        #Checks if the datum exists in the DB
        datum = None
        for d in run['data']:
            if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == rse:
                datum = d
                break
            
        #Delete the datum
        if datum is not None:
            self.db.RemoveDatafield(run['_id'],datum)
            print("Datum deleted in DB.")
        else:
            print('There is no datum to delete')

        #Get the rule of a given DID
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=rse)

        #Delete the rule
        if rucio_rule['exists']:
            self.rucio_client.update_replication_rule(rucio_rule['id'], {'locked' : False})
            self.rc.DeleteRule(rucio_rule['id'])
            print("Rucio rule deleted.")
        else:
            print('There is no Rucio rule to delete')

        #In case it is datamanager, directly delete files
        if rse == self.UPLOAD_TO:
            files = list_file_replicas(number, dtype, hash, self.UPLOAD_TO)
            print("Deleting rucio data in datamanager disk. Deleting",len(files),"files")
            for file in files:
                try:
                    os.remove(file)
                except:
                    print("File: {0} not found".format(file))

        print("Done.")



    def create_upload_rules(self,did):

        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)

        dtype = did.split('-')[0].split(':')[-1]

        # Fourth action: creating the rules abroad
        if rucio_rule['exists'] and rucio_rule['state']=="OK" :
            print("Adding the Rucio rules abroad...")

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

            if dtype in self.RECORDS_TYPES:
                rses = rses + self.RECORDS_RSES

            for from_rse, to_rse in zip(rses, rses[1:]):
                to_rule = self.rc.GetRule(upload_structure=did, rse=to_rse)
                if not to_rule['exists']:
                    print("Rule from {0} to {1}".format(from_rse,to_rse))
                    self.add_rule(did, from_rse, to_rse)




    def fix_upload(self,did):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Fixing the upload associated to the DID: {0}".format(did))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number' : number})

        # Gets the status
        if 'status' in run:
            print('Run status: {0}'.format(run['status']))
        else:
            print('Run status: {0}'.format('Not available'))

        # Extracts the correct Event Builder machine who processed this run
        # Then also the bootstrax state and, in case it was abandoned, the reason
        if 'bootstrax' in run:
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]
        else:
            print('Not processed')
            return(0)
                
        print("EB: {0}".format(eb))

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        if datum is None:
            print('There is no EB datum. No fix is possible')
            return(0)

        if ebstatus != "":
            print('EB status: {0}'.format(ebstatus))
        else:
            print('EB status: not available')

        # Get the expected number of files
        Nfiles = -1
        if 'file_count' in datum:
            Nfiles = datum['file_count']


        # First action: remove files in datamanager no matter if they were already uploaded or not
        print("Removing all files so far uploaded (successfully or not) in datamanager...")
        filelistname = os.path.join("/archive/data/rucio/xnt_%06d/*/*/" % number, dtype+"-"+hash+"*")
        filelist = glob.glob(filelistname)
        for filePath in filelist:
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)


        # Second action: complete the missing uploads on the existing rule
        print('Resuming the upload...')
        file = datum['location'].split('/')[-1]
        upload_path = os.path.join(self.DATADIR, eb, file)
        self.rc.UploadToDid(did, upload_path, self.UPLOAD_TO)

        # Third action: check if datum in DB does not exist. If not, add it and mark the EB datum as transferred
        datum_upload = None
        for d in run['data']:
            if 'did' in d:
                if d['did'] == did and d['host'] == 'rucio-catalogue' and d['location'] == self.UPLOAD_TO:
                    datum_upload = d
                    break
        if datum_upload is None:
            print('The datum concerning data type {0} and site {1} is missing in the DB. It will be added'.format(did,self.UPLOAD_TO))

            # Add a new data field with LNGS as RSE and with status "transferred"
            data_dict = datum.copy()
            data_dict.update({'host': "rucio-catalogue",
                              'type': dtype,
                              'location': "LNGS_USERDISK",
                              'lifetime': 0,
                              'status': 'transferred',
                              'did': did,
                              'protocol': 'rucio'
                          })
            self.db.AddDatafield(run['_id'], data_dict)

        
        # Fourth action: update the eb data entry with status "transferred"
        self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
                                       { '$set': { "data.$.status" : "transferred" } })

        # Fifth action: in case the rule itself is missing, this would create it
        rucio_rule = self.rc.GetRule(upload_structure=did, rse=self.UPLOAD_TO)
        #print(rucio_rule)
        if not rucio_rule['exists']:
            print('Even if files have been uploaded, the rule has not been created yet. Creating it...')
            did_dictionary = [{'scope' : did.split(':')[0], 'name' : did.split(':')[1]}]
            replicas = list(self.replicaclient.list_replicas(did_dictionary,rse_expression=self.UPLOAD_TO))
            if len(replicas)!=Nfiles:
                print('Error: the rule cannot be created beause the number of files uploaded ({0}) is different from the expected one ({1})'.format(len(replicas),Nfiles))
                return(0)
            if rucio_rule['exists']:
                print('Error: the rule cannot be created beause it exists already')
                return(0)
            os.system('rucio add-rule {0} 1 {1}'.format(did,self.UPLOAD_TO))

        # Sixth action: creating the rules abroad
        self.create_upload_rules(did)

        return(0)



    def upload(self,path,rse):

        directory = path.split('/')[-1]
        number = int(directory.split('-')[0])
        dtype = directory.split('-')[1]
        hash = directory.split('-')[2]
        did = make_did(number, dtype, hash)

        print("Uploading did {0} in {1} from path {2}".format(did,rse,path))

        self.rc.UploadToDid(did, path, rse)

        return(0)



    def delete_db_datum(self,did,site):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Removing the datum from DB for the DID: {0} and from the site {1}".format(did,site))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))
        print("Site: {0}".format(site))

        run = self.db.db.find_one({'number' : number})

        # Get the EB datum and its status
        datum = None
        for d in run['data']:
            if 'eb' in site:
                if d['type'] == dtype and site in d['host'] and 'xenon.local' in d['host']:
                    datum = d
                    break
            else:
                if d['type'] == dtype and d['host']=='rucio-catalogue' and d['location']==site:
                    datum = d
                    break
                

        if datum is not None:
            self.db.RemoveDatafield(run['_id'],datum)
            print("Done.")
        else:
            print('There is no datum. Nothing has been deleted')


    def set_run_status(self,number,status):

        number = int(number)

        print("Setting the status of run {0} to the value {1}".format(number,status))

        run = self.db.db.find_one({'number' : number})
        print("status before = ",run['status'])

        self.db.db.find_one_and_update({'_id': run['_id']},{'$set':{"status": status}})

        run = self.db.db.find_one({'number' : number})
        print("status after = ",run['status'])


    def set_eb_status(self,did,status):

        print("Setting the EB status of DID {0} to the value {1}".format(did,status))

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number' : number})

        # Extracts the correct Event Builder machine who processed this run
        # Then also the bootstrax state and, in case it was abandoned, the reason
        if 'bootstrax' in run:
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]
        else:
            print('Not processed')
            return(0)

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        if datum is None:
            print('There is no EB datum.')
            return(0)

        if ebstatus != "":
            print("EB status before = ",ebstatus)
        else:
            print("EB status absent before")

        #Set the aimed value
        #        self.db.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': datum}},
        #                                       {'$set': {'data.$.status': status}})

        self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
                                       { '$set': { "data.$.status" : status } })


        run = self.db.db.find_one({'number' : number})

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        print("EB status after = ",ebstatus)



    def list_non_transferred_runs(self):
        
        runs = self.db.db.find({'status' : "transferring"},{'number' : 1, 'data' : 1})
    
#        dtypes = ["records","records_he", "records_nv", "records_mv"]
#        dtypes = ["records_nv"]
        dtypes = ["raw_records"]

        for run in runs:
            
            for d in run['data']:
                if d['type'] in dtypes and d['host'] == 'rucio-catalogue' and d['location'] == 'LNGS_USERDISK':
                    print(run['number'],d['did'],d['status']," ",end='')
                    for deb in run['data']:
                        if deb['type'] == d['type'] and 'eb' in deb['host']:
                            print(deb['host'],deb['status'],end='')
                    print("")



    def get_datasets_size(self):

        rucio_client = Client()

        runs = self.db.db.find({
#            'number': {"$gte": 40000, "$lt": 40005},
            'number': {"$gte": 10000},
#            'number': {"$lt": 48475},
            'status': 'transferred'
        },
                               {'_id': 1, 'number': 1}).sort("number", pymongo.ASCENDING)

        for r in runs:

            run = self.db.db.find_one({'_id' : r['_id']},{'_id': 1, 'number': 1, 'mode': 1, 'data': 1, 'bootstrax': 1})

            number = run['number']
            mode = run['mode']

            dids = set()
            for d in run['data']:
                if d['host'] == 'rucio-catalogue':
                    did = d['did'].split('.')[0]
                    dids.add(did)

            for did in dids:

                if "raw_records" not in did:
                    continue

                scope = did.split(':')[0]
                dataset = did.split(':')[1]
                files = list(rucio_client.list_files(scope,dataset))
                size = 0
                for ifile in files:
                    size = size + ifile['bytes']
                rules = rucio_client.list_did_rules(scope,dataset)
                for rule in rules:
                    print(number,mode,d['type'],len(files),size,0,rule['rse_expression'])
                rules = rucio_client.list_did_rules(scope,dataset+".tarball")
                for rule in rules:
                    print(number,mode,d['type'],len(files),size,1,rule['rse_expression'])


    def test(self):

#        runs = self.db.db.find({'status' : "transferring"},{'number' : 1, 'data' : 1})
        self.get_datasets_size()


    def test_db_modification(self, did, new_status_name):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Testing how quickly a modification in DB is registered. Using DID: {0}".format(did))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number': number})

        # Gets the status
        if 'status' in run:
            print('Run status: {0}'.format(run['status']))
        else:
            print('Run status: {0}'.format('Not available'))

        # Extracts the correct Event Builder machine who processed this run
        # Then also the bootstrax state and, in case it was abandoned, the reason
        if 'bootstrax' in run:
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]
        else:
            print('Not processed')
            return (0)

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        if datum is None:
            print('There is no EB datum. No reset is possible')
            return (0)

        if ebstatus != "":
            print('EB status: {0}'.format(ebstatus))
        else:
            print('EB status: not available')


        # Start the changes: set the EB status as 'eb_ready_to_upload'
        self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
                                       { '$set': { "data.$.status" : new_status_name} })
        print("EB status changed to {0}".format(new_status_name))

        # Reload the run
        run = self.db.db.find_one({'number': number})

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        # Prints the eb status as a confirmation of the performed change
        if ebstatus != "":
            print('New EB status: {0}'.format(ebstatus))
        else:
            print('New EB status: not available')

    def __del__(self):
        pass



    def fix_upload_db(self,did):

        hash = did.split('-')[-1]
        dtype = did.split('-')[0].split(':')[-1]
        number = int(did.split(':')[0].split('_')[-1])

        print("Fixing the upload associated to the DID: {0}".format(did))
        print("Run number: {0}".format(number))
        print("Data type: {0}".format(dtype))
        print("Hash: {0}".format(hash))

        run = self.db.db.find_one({'number' : number})

        # Gets the status
        if 'status' in run:
            print('Run status: {0}'.format(run['status']))
        else:
            print('Run status: {0}'.format('Not available'))

        # Extracts the correct Event Builder machine who processed this run
        # Then also the bootstrax state and, in case it was abandoned, the reason
        if 'bootstrax' in run:
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]
        else:
            print('Not processed')
            return(0)

        #Checks if the LNGS datum exists already in the DB
        for d in run['data']:
            if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == "LNGS_USERDISK":
                print('The datum concerning did {0} for location {1} is already present in DB. Forced to stop'.format(did,"LNGS_USERDISK"))
                return(0)
                

        # Get the EB datum and its status
        ebstatus = ""
        datum = None
        for d in run['data']:
            if d['type'] == dtype and eb in d['host']:
                datum = d
                if 'status' in d:
                    ebstatus = d['status']

        if datum is None:
            print('There is no EB datum. No fix is possible')
            return(0)

        # Update the eb data entry with status "transferred"
        self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
                                       { '$set': { "data.$.status" : "transferred"} })

        # Add a new data field with LNGS as RSE and with status "trasferred"
        data_dict = datum.copy()
        data_dict.update({'host': "rucio-catalogue",
                          'type': dtype,
                          'location': "LNGS_USERDISK",
                          'lifetime': 0,
                          'status': 'transferred',
                          'did': did,
                          'protocol': 'rucio'
                      })
        self.db.AddDatafield(run['_id'], data_dict)

        if ebstatus != "":
            print('EB status: {0}'.format(ebstatus))
        else:
            print('EB status: not available')

        print('Done')







    def postpone(self):

        # Get the current screen session
        process = psutil.Process()
        screen = process.parent().parent().parent().parent().cmdline()[-1]

        # Take the tmp file of this session containing the dataset information
        filename = "/tmp/admix-"+screen

        # Destination name
        suffix = time.strftime("-%Y-%m-%d-%H-%M-%S",time.localtime(time.time()))
        destination_path = helper.get_hostconfig()['path_datasets_to_fix']+"/"
        new_filename = destination_path+filename.split('/')[-1]+suffix

        if os.path.isfile(filename) and os.path.isdir(destination_path):
            shutil.move(filename,new_filename)
            print("Dataset postponed by moving file {0} to {1}".format(filename,new_filename))



    

def main():
    parser = ArgumentParser("admix-fix")

    config = Config()

    parser.add_argument("--tarball_all", nargs=2, help="Tarballs all data from a given FROM_RSE and upload them to TO_RSE", metavar=('FROM_RSE','TO_RSE'))
    parser.add_argument("--tarball", nargs=3, help="Extracts data with a given DID from a given FROM_RSE, tarballs it, then uploads it in TO_RSE", metavar=('DID','FROM_RSE','TO_RSE'))
    parser.add_argument("--bring_online", nargs=2, help="Pre-stages all files belonging to a DID for a given RSE", metavar=('DID','RSE'))
    parser.add_argument("--clean_empty_directories", nargs=1, help="Removes all empty sub-directories of a given directory DIR", metavar=('DIR'))
    parser.add_argument("--clean_empty_directories_rse", nargs=1, help="Removes all empty sub-directories of a given RSE", metavar=('RSE'))

    parser.add_argument("--reset_upload", nargs=1, help="Deletes everything related a given DID, except data in EB. The deletion includes the entries in the Rucio catalogue and the related data in the DB rundoc. This is ideal if you want to retry an upload that failed", metavar=('DID'))
    parser.add_argument("--fix_upload", nargs=1, help="Deletes everything related a given DID, then it retries the upload", metavar=('DID'))
    parser.add_argument("--upload", nargs=2, help="Uploads a dataset on a given RSE", metavar=('PATH','RSE'))
    parser.add_argument("--add_rule", nargs=3, help="Add a new replication rule of a given DID from one RSE to another one. The rundoc in DB is updated with a new datum as well", metavar=('DID','FROM_RSE','TO_RSE'))
    parser.add_argument("--add_db_rule_tar", nargs=3, help="Add a new data entry in a rundoc with the tar version of a given DID and destination TO_RSE, using FROM_RSE as base", metavar=('DID','FROM_RSE','TO_RSE'))
    parser.add_argument("--delete_rule", nargs=2, help="Delete a replication rule of a given DID from one RSE. The rundoc in DB is deleted as well", metavar=('DID','RSE'))
    parser.add_argument("--delete_db_datum", nargs=2, help="Deletes the db datum corresponding to a given DID. The SITE can be either a specific EB machine (ex: eb1) or a specific RSE", metavar=('DID','SITE'))

    parser.add_argument("--set_run_status", nargs=2, help="Set the run status to a given NAME (typical case is to set it to eb_ready_to_upload)", metavar=('RUN_NUMBER','STATUS_NAME'))
    parser.add_argument("--set_eb_status", nargs=2, help="Set the EB status of a given DID to a given NAME", metavar=('DID','STATUS_NAME'))

    parser.add_argument("--priority", type=int, help="Priority to assign to Rucio rules (default: %(default)s)", default=3)
    parser.add_argument("--skip_rucio", help="Add this flag in context of add_rule in case you just want to update DB since Rucio rule exists already", action='store_true')
    parser.add_argument("--ignore_db_errors", help="Add this flag in context of add_rule in case you just want to proceed on creating a rule even if the DB does not contain the source data", action='store_true')

    parser.add_argument("--no_wait", help="Add this flag in context of bring_online in case you don't want to wait for the staging of the files", action='store_true')
    parser.add_argument("--list_non_transferred_runs", help="Lists all runs whose status is still not transferred", action='store_true')

    parser.add_argument("--test_db_modification", nargs=2, help="Test how quickly a modification in DB is registered", metavar=('DID','STATUS'))
    parser.add_argument("--fix_upload_db", nargs=1, help="To be used when the upload done by Rucio has been completed but then admix crashed before updating the DB", metavar=('DID'))
    parser.add_argument("--create_upload_rules", nargs=1, help="To be used when the upload done by Rucio has been completed but then admix crashed before creating the rules abroad", metavar=('DID'))
    parser.add_argument("--postpone", help="To be used when an upload failed (for any reason) in a screen session and you want to free the session. Metadata on the failed dataset are copied in a directory and will be fixed by an expert", action='store_true')
    parser.add_argument("--add_rules_from_file", nargs=3, help="To be used when you want to transfer data from one RSE to another RSE, using rucio and without updating the database. The option requires a FILE containing the list of DIDs to be transferred. Each rule is copied only after the previous one is successfully completed. This is particularly suggested for tapes", metavar=('FILE','FROM_RSE','TO_RSE'))

    parser.add_argument("--test", help="It's a test. Never use it",action='store_true')

    args = parser.parse_args()


    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))

    fix = Fix()

    fix.skip_rucio = args.skip_rucio
    fix.ignore_db_errors = args.ignore_db_errors
    fix.no_wait = args.no_wait
    fix.priority = args.priority

    try:
        if args.tarball_all:
            fix.tarball_all(args.tarball_all[0],args.tarball_all[1])
        if args.tarball:
            fix.tarball(args.tarball[0],args.tarball[1],args.tarball[2])
        if args.bring_online:
            fix.bring_online(args.bring_online[0],args.bring_online[1])
        if args.clean_empty_directories:
            fix.clean_empty_directories(args.clean_empty_directories[0])
        if args.clean_empty_directories_rse:
            fix.clean_empty_directories_rse(args.clean_empty_directories_rse[0])
        if args.reset_upload:
            fix.reset_upload(args.reset_upload[0])
        if args.fix_upload:
            fix.fix_upload(args.fix_upload[0])
        if args.upload:
            fix.upload(args.upload[0],args.upload[1])
        if args.add_rule:
            fix.add_rule(args.add_rule[0],args.add_rule[1],args.add_rule[2])
        if args.add_rules_from_file:
            fix.add_rules_from_file(args.add_rules_from_file[0],args.add_rules_from_file[1],args.add_rules_from_file[2])
        if args.add_db_rule_tar:
            fix.add_db_rule_tar(args.add_db_rule_tar[0],args.add_db_rule_tar[1],args.add_db_rule_tar[2])
        if args.delete_rule:
            fix.delete_rule(args.delete_rule[0],args.delete_rule[1])
        if args.delete_db_datum:
            fix.delete_db_datum(args.delete_db_datum[0],args.delete_db_datum[1])

        if args.set_run_status:
            fix.set_run_status(args.set_run_status[0],args.set_run_status[1])
        if args.set_eb_status:
            fix.set_eb_status(args.set_eb_status[0],args.set_eb_status[1])

        if args.list_non_transferred_runs:
            fix.list_non_transferred_runs()

        if args.test:
            fix.test()

        if args.test_db_modification:
            fix.test_db_modification(args.test_db_modification[0],args.test_db_modification[1])

        if args.fix_upload_db:
            fix.fix_upload_db(args.fix_upload_db[0])

        if args.create_upload_rules:
            fix.create_upload_rules(args.create_upload_rules[0])

        if args.postpone:
            fix.postpone()

#        if args.action == "reset_upload" and args.did:
#            fix.reset_upload(args.did)
#        if args.action == "add_rule" and args.did and args.fromrse and args.torse:
#            fix.add_rule(args.did,args.fromrse,args.torse)
        print("")
    except KeyboardInterrupt:
        return 0

