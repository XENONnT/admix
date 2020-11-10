import os
import time
import shutil
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.keyword import Keyword
from admix.utils.list_file_replicas import list_file_replicas
from admix.utils import make_did
import admix.helper.helper as helper
import utilix

from admix.interfaces.database import ConnectMongoDB
from utilix.config import Config

def get_data_docs_from_rucio(run_id):

    ddocs = []

    for rse in ['LNGS_USERDISK','UC_OSG_USERDISK','CCIN2P3_USERDISK']:
        ddoc = [
            {
                'host': 'rucio-catalogue',
                'type': 'raw_records',
                'location': rse,
                'lifetime': None,
                'status': 'transferred',
                'did': 'xnt_'+run_id+':raw_records-rfzvpzj4mf',
                'protocol': 'rucio'
            },
            {
                'host': 'rucio-catalogue',
                'type': 'raw_records_he',
                'location': rse,
                'lifetime': None,
                'status': 'transferred',
                'did': 'xnt_'+run_id+':raw_records_he-rfzvpzj4mf',
                'protocol': 'rucio'
            },
            {
                'host': 'rucio-catalogue',
                'type': 'raw_records_aqmon',
                'location': rse,
                'lifetime': None,
                'status': 'transferred',
                'did': 'xnt_'+run_id+':raw_records_aqmon-rfzvpzj4mf',
                'protocol': 'rucio'
            },
            {
                'host': 'rucio-catalogue',
                'type': 'raw_records_mv',
                'location': rse,
                'lifetime': None,
                'status': 'transferred',
                'did': 'xnt_'+run_id+':raw_records_mv-rfzvpzj4mf',
                'protocol': 'rucio'
            },
            {
                'host': 'rucio-catalogue',
                'type': 'records',
                'location': rse,
                'lifetime': None,
                'status': 'transferred',
                'did': 'xnt_'+run_id+':records-56ausr64s7',
                'protocol': 'rucio'
            }
        ]
        ddocs.extend(ddoc)

    return ddocs





def fixdb(number):
#    config = Config()
#    config.get('Admix','config_file')

    DB = ConnectMongoDB()

    rundoc = DB.db.find_one({'number' : number})

    print(rundoc['number'])
    print(rundoc['status'])

    run_id = "%06d" % number

    # If in Rucio, change the status field to transferred if we actually have stored the run in rucio
    print("Setting run",number,"as transferred")
#    DB.db.find_one_and_update({'number':number},{'$set':{"status": "transferred"}})
    DB.db.find_one_and_update({'number':number},{'$set':{"status": "eb_ready_to_upload"}})



    ddocs = get_data_docs_from_rucio(run_id)
    if not type(ddocs) == list:
        raise ValueError("check the format as in docstring get_data_docs_from_rucio")

    for ddoc in ddocs:
        # Add this ddoc (one location of data in rucio) to the data field (i.e. list)
        # in the rundoc.
        print("Adding",ddoc)
#        DB.db.update_one({'_id': rundoc['_id']},{"$addToSet": {'data': ddoc}})





def remove_datatype_from_db_and_datamanager(did):

    config = Config()
    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))
    RSES = helper.get_hostconfig()['rses']


    DB = ConnectMongoDB()
    rc = RucioSummoner()

    hash = did.split('-')[-1]
    dtype = did.split('-')[0].split(':')[-1]
    number = int(did.split(':')[0].split('_')[-1])

    print("Removing",number,dtype,hash)


    rundoc = DB.db.find_one({'number' : number})

    print("status = ",rundoc['status'])

    run_id = "%06d" % number

    # make it uploadable
    DB.db.find_one_and_update({'number':number},{'$set':{"status": "eb_ready_to_upload"}})

    # Remove DB entries for all RSEs
    for d in rundoc['data']:
        if d['type'] == dtype and d['host'] == 'rucio-catalogue':

            # Remove the data entry in DB
            print("Deleting data = ",d)
            DB.db.update_one({"_id" : rundoc['_id']},
                              {"$pull" : {"data" : d} })

    # Remove Rucio rules for all RSEs
    for rse in RSES:

        rucio_rule = rc.GetRule(upload_structure=did, rse=rse)
        if rucio_rule['exists']:
            print("Deleting rucio rule = ", rucio_rule['id'])
            rc.DeleteRule(rucio_rule['id'])


    files = list_file_replicas(number, dtype, hash, "LNGS_USERDISK")
    print("Deleting rucio data in datamanager disk. Deleting",len(files),"files")
    for file in files:
        os.remove(file)



def change_status(number,status):

    DB = ConnectMongoDB()

    print("Run ",number)

    rundoc = DB.db.find_one({'number' : number})
    print("status before = ",rundoc['status'])

    run_id = "%06d" % number

    DB.db.find_one_and_update({'number':number},{'$set':{"status": status}})

    rundoc = DB.db.find_one({'number' : number})
    print("status after = ",rundoc['status'])


def showcontexts():

    config = Config()
    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))

    #Define data types
    NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
    RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
    RECORDS_DTYPES = helper.get_hostconfig()['records_types']

    #Get other parameters
    DATADIR = helper.get_hostconfig()['path_data_to_upload']
    periodic_check = helper.get_hostconfig()['upload_periodic_check']
    RSES = helper.get_hostconfig()['rses']

    #Init the runDB
    db = ConnectMongoDB()

    data_types = RAW_RECORDS_DTYPES + RECORDS_DTYPES + NORECORDS_DTYPES


    context = 'xenonnt_online'

    for dtype in data_types:
        hash = utilix.db.get_hash(context, dtype)
        print('Data type {0}, hash {1}'.format(dtype,hash))


def dummy():
    return(0)



if __name__ == "__main__":

    #[8355, 8356, 8357, 8358, 8359, 8360, 8361, 8362, 8363, 8364, 8365, 8366]

    #Indeed up to 8360, data were already uploaded and transferred outside. I need to patch them.
    #8361 was interrupted during the issue, so for this I will remove everything in Rucio and restart the upload from scratch).
    #Then, I restarted admix in the usual way from 8362 and it is going fine

#    remove_datatype_from_db_and_datamanager("xnt_008650:records-jxkqp76kam")
#    remove_datatype_from_db_and_datamanager("xnt_008650:lone_hits-b7dgmtzaef")
#    remove_datatype_from_db_and_datamanager("xnt_008013:raw_records-rfzvpzj4mf")
#    remove_datatype_from_db_and_datamanager("xnt_008620:records-jxkqp76kam")
#    change_status(9492,"eb_ready_to_upload")
    change_status(9492,"transferred")
#    change_status(8896,"uploading")
#    change_status(7151,"")
#    fixdb(8650)
#    dummy()
#    showcontexts()
