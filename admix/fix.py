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
from utilix.config import Config
import utilix
from bson.json_util import dumps
from datetime import timezone, datetime, timedelta
import pymongo
import glob

from pymongo import ReturnDocument

class Fix():

    def __init__(self):

        #Take all data types categories
        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.LIGHT_RAW_RECORDS_DTYPES = helper.get_hostconfig()['light_raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        #Choose which data type you want to treat
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES + self.LIGHT_RAW_RECORDS_DTYPES
        
        #Take the list of all XENON RSEs
        self.RSES = helper.get_hostconfig()['rses']

        #Take the RSE that is used to perform the upload
        self.UPLOAD_TO = helper.get_hostconfig()['upload_to']

        #Take the directory where datamanager has to upload data
        self.DATADIR = helper.get_hostconfig()['path_data_to_upload']

        # Get the sequence of rules to be created according to the data type
        self.RAW_RECORDS_RSES = helper.get_hostconfig()["raw_records_rses"]
        self.LIGHT_RAW_RECORDS_RSES = helper.get_hostconfig()["light_raw_records_rses"]
        self.NORECORDS_RSES = helper.get_hostconfig()["norecords_rses"]
        self.RECORDS_RSES = helper.get_hostconfig()["records_rses"]


        #Init the runDB
        self.db = ConnectMongoDB()

        #Init Rucio for later uploads and handling:
        self.rc = RucioSummoner()

        #Rucio Rule assignment priority
        self.priority = 3



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
            print('The datum concerning data type {0} and site {1} is missing in the DB. Forced to stop'.format(dtype,from_rse))
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

            # Update the eb data entry with status "transferred"
            self.db.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': {'type' : datum['type'], 'location' : datum['location'], 'host' : datum['host'] }}},
                                           { '$set': { "data.$.status" : "transferred" } })

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

        # Fourth action: creating the rules abroad
        print("Adding the Rucio rules abroad...")
        rses = [self.UPLOAD_TO]
        if dtype in self.NORECORDS_DTYPES:
            rses = rses + self.NORECORDS_RSES
        if dtype in self.RAW_RECORDS_DTYPES:
            rses = rses + self.RAW_RECORDS_RSES
        if dtype in self.LIGHT_RAW_RECORDS_DTYPES:
            rses = rses + self.LIGHT_RAW_RECORDS_RSES
        if dtype in self.RECORDS_DTYPES:
            rses = rses + self.RECORDS_RSES

        for from_rse, to_rse in zip(rses, rses[1:]):
            print("Rule from {0} to {1}".format(from_rse,to_rse))
            self.add_rule(did, from_rse, to_rse)

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

    def test(self):
        runs = self.db.db.find({'number' : "transferring"},{'number' : 1, 'data' : 1})
#        self.db.db.find_one_and_update({'number': 23838, 'deleted_data.type' : 'raw_records' },
#                          { '$set': { "deleted_data.$.file_count" : 44 } })


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

    parser.add_argument("--reset_upload", nargs=1, help="Deletes everything related a given DID, except data in EB. The deletion includes the entries in the Rucio catalogue and the related data in the DB rundoc. This is ideal if you want to retry an upload that failed", metavar=('DID'))
    parser.add_argument("--fix_upload", nargs=1, help="Deletes everything related a given DID, then it retries the upload", metavar=('DID'))
    parser.add_argument("--add_rule", nargs=3, help="Add a new replication rule of a given DID from one RSE to another one. The rundoc in DB is updated with a new datum as well", metavar=('DID','FROM_RSE','TO_RSE'))
    parser.add_argument("--delete_rule", nargs=2, help="Delete a replication rule of a given DID from one RSE. The rundoc in DB is deleted as well", metavar=('DID','RSE'))
    parser.add_argument("--delete_db_datum", nargs=2, help="Deletes the db datum corresponding to a given DID. The SITE can be either a specific EB machine (ex: eb1) or a specific RSE", metavar=('DID','SITE'))

    parser.add_argument("--set_run_status", nargs=2, help="Set the run status to a given NAME (typical case is to set it to eb_ready_to_upload)", metavar=('RUN_NUMBER','STATUS_NAME'))
    parser.add_argument("--set_eb_status", nargs=2, help="Set the EB status of a given DID to a given NAME", metavar=('DID','STATUS_NAME'))

    parser.add_argument("--priority", type=int, help="Priority to assign to Rucio rules (default: %(default)s)", default=3)
    parser.add_argument("--skip_rucio", help="Add this flag in context of add_rule in case you just want to update DB since Rucio rule exists already", action='store_true')
    parser.add_argument("--list_non_transferred_runs", help="Lists all runs whose status is still not transferred", action='store_true')

    parser.add_argument("--test_db_modification", nargs=2, help="Test how quickly a modification in DB is registered", metavar=('DID','STATUS'))
    parser.add_argument("--fix_upload_db", nargs=1, help="To be used when the upload done by Rucio has been completed but then admix crashed before updating the DB. Note: it's up to you to create the rules to ship data abroad", metavar=('DID'))
    parser.add_argument("--postpone", help="To be used when an upload failed (for any reason) in a screen session and you want to free the session. Metadata on the failed dataset are copied in a directory and will be fixed by an expert", action='store_true')

    parser.add_argument("--test", help="It's a test. Never use it",action='store_true')

    args = parser.parse_args()


    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))

    fix = Fix()

    fix.skip_rucio = args.skip_rucio
    fix.priority = args.priority

    try:
        if args.reset_upload:
            fix.reset_upload(args.reset_upload[0])
        if args.fix_upload:
            fix.fix_upload(args.fix_upload[0])
        if args.add_rule:
            fix.add_rule(args.add_rule[0],args.add_rule[1],args.add_rule[2])
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

        if args.postpone:
            fix.postpone()

#        if args.action == "reset_upload" and args.did:
#            fix.reset_upload(args.did)
#        if args.action == "add_rule" and args.did and args.fromrse and args.torse:
#            fix.add_rule(args.did,args.fromrse,args.torse)
        print("")
    except KeyboardInterrupt:
        return 0

