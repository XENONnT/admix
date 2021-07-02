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



def remove_datatype_from_db_and_rule(number,dtype,rse):

    config = Config()
    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))
    RSES = helper.get_hostconfig()['rses']


    DB = ConnectMongoDB()
    rc = RucioSummoner()

    print("Removing",number,dtype)


    rundoc = DB.db.find_one({'number' : number})

    print("status = ",rundoc['status'])

    run_id = "%06d" % number

    # make it uploadable
#    DB.db.find_one_and_update({'number':number},{'$set':{"status": "transferring"}})


    for d in rundoc['data']:
        if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == rse:

            # Remove the data entry in DB
            print("Deleting datum from DB = ",d)
            DB.db.update_one({"_id" : rundoc['_id']},
                              {"$pull" : {"data" : d} })

            # Remove Rucio rule
            rucio_rule = rc.GetRule(upload_structure=d['did'], rse=rse)
            if rucio_rule['exists']:
                print("Deleting rucio rule = ", rucio_rule['id'])
                rc.DeleteRule(rucio_rule['id'])





def remove_datatype_from_db(did,rse):

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
    DB.db.find_one_and_update({'number':number},{'$set':{"status": "transferring"}})

    # Remove DB entries for all RSEs
    for d in rundoc['data']:
        if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['did'] == did and d['location'] == rse:

            # Remove the data entry in DB
            print("Deleting data = ",d)
            DB.db.update_one({"_id" : rundoc['_id']},
                              {"$pull" : {"data" : d} })

    # Remove Rucio rules for all RSEs
#    for rse in RSES:

#        rucio_rule = rc.GetRule(upload_structure=did, rse=rse)
#        if rucio_rule['exists']:
#            print("Deleting rucio rule = ", rucio_rule['id'])
#            rc.DeleteRule(rucio_rule['id'])



def reset_upload(did):

    config = Config()
    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))
    RSES = helper.get_hostconfig()['rses']


    DB = ConnectMongoDB()
    rc = RucioSummoner()

    hash = did.split('-')[-1]
    dtype = did.split('-')[0].split(':')[-1]
    number = int(did.split(':')[0].split('_')[-1])

    print("Resetting the upload associated to the DID: {0}".format(did))
    print("Run number: {0}".format(number))
    print("Data type: {0}".format(dtype))
    print("Hash: {0}".format(hash))

    run = DB.db.find_one({'number' : number})

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
    #    DB.db.find_one_and_update({'number':number},{'$set':{"status": "transferring"}})


    # First action: remove the files stored in datamanager
    files = list_file_replicas(number, dtype, hash, "LNGS_USERDISK")
    print("Deleting rucio data in datamanager disk. Deleting",len(files),"files")
    for file in files:
        os.remove(file)



    # Second action: remove the LNGS Rucio rule
    rucio_rule = rc.GetRule(upload_structure=did, rse='LNGS_USERDISK')
    if rucio_rule['exists']:
        print("Deleting rucio rule = ", rucio_rule['id'])
        rc.DeleteRule(rucio_rule['id'])
        # Wait for 1 hour (plus 5 minutes of margin)
        delay = 3600+60*5
        print("We have to wait for {0} seconds before proceeding to the next step".format(delay))
        time.sleep(delay)
    else:
        print("There is no rule to delete")



    # Third action: set the EB status as 'eb_ready_to_upload' 
    DB.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': datum}},
                                   {'$set': {'data.$.status': 'eb_ready_to_upload'}})
    print("EB status changed to eb_ready_to_upload")



    # Reload the run
    run = DB.db.find_one({'number' : number})

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


    # Remove the LNGS DB entry
#    for d in run['data']:
#        if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == 'LNGS_USERDISK':
#            # Remove the data entry in DB
#            print("Deleting data = ",d)
#            DB.db.update_one({"_id" : run['_id']},
#                              {"$pull" : {"data" : d} })







def add_rule(did,from_rse,to_rse,priority=3):

    config = Config()
    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))
    RSES = helper.get_hostconfig()['rses']


    DB = ConnectMongoDB()
    rc = RucioSummoner()

    hash = did.split('-')[-1]
    dtype = did.split('-')[0].split(':')[-1]
    number = int(did.split(':')[0].split('_')[-1])

    print("Adding a new rule {0} from {1} to {2}".format(did,from_rse,to_rse))
    print("Run number: {0}".format(number))
    print("Data type: {0}".format(dtype))
    print("Hash: {0}".format(hash))

    run = DB.db.find_one({'number' : number})

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
            continue
    if datum is None:
        print('The datum concerning data type {0} and site {1} is missing in the DB. Forced to stop'.format(dtype,from_rse))
        return(0)


    # Checks the rule status of the sender RSE
    rucio_rule = rc.GetRule(upload_structure=did, rse=from_rse)
    if rucio_rule['state'] != 'OK':
        print('The rule in {0} is not OK. Forced to stop'.format(from_rse))
        return(0)

    # set the new rule
    result = rc.AddConditionalRule(did, from_rse, to_rse, lifetime=None, priority=priority)
    rucio_rule = rc.GetRule(did, rse=to_rse)

    # Update run status
    DB.db.find_one_and_update({'number': number},{'$set': {'status': 'transferring'}})

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
    DB.AddDatafield(run['_id'], data_dict)




def add_rule_run_dtype(number,dtype,from_rse,to_rse,priority=3):

    config = Config()
    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))
    RSES = helper.get_hostconfig()['rses']


    DB = ConnectMongoDB()
    rc = RucioSummoner()

    print("Adding a new rule from {0} to {1}".format(from_rse,to_rse))
    print("Run number: {0}".format(number))
    print("Data type: {0}".format(dtype))

    run = DB.db.find_one({'number' : number})

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
            continue
    if datum is None:
        print('The datum concerning data type {0} and site {1} is missing in the DB. Forced to stop'.format(dtype,from_rse))
        return(0)

    print("DID: {0}".format(datum['did']))


    #Checks if the destination datum exists already in the DB
    for d in run['data']:
        if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == to_rse:
            print('Rule already exists')
            return(0)


    # Checks the rule status of the sender RSE
    rucio_rule = rc.GetRule(upload_structure=datum['did'], rse=from_rse)
    if rucio_rule['state'] != 'OK':
        print('The rule in {0} is not OK. Forced to stop'.format(from_rse))
        return(0)

    # set the new rule
    result = rc.AddConditionalRule(datum['did'], from_rse, to_rse, lifetime=None, priority=priority)
    rucio_rule = rc.GetRule(datum['did'], rse=to_rse)

    # Update run status
    DB.db.find_one_and_update({'number': number},{'$set': {'status': 'transferring'}})

    # Add a new datum in the run document
    updated_fields = {'host': "rucio-catalogue",
                      'type': dtype,
                      'location': to_rse,
                      'lifetime': rucio_rule['expires'],
                      'status': 'transferring',
                      'did': datum['did'],
                      'protocol': 'rucio'
            }
    data_dict = datum.copy()
    data_dict.update(updated_fields)
    DB.AddDatafield(run['_id'], data_dict)




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

    """
    runs = [ "010301", "010302", "010303", "010304", "010305", "010321", "010322", "010323", "010324", "010325", "010326", "010327"]
    for run in runs:
        did = "xnt_"+run+":raw_records-rfzvpzj4mf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        remove_datatype_from_db(did,"CNAF_TAPE2_USERDISK")
        did = "xnt_"+run+":raw_records_he-rfzvpzj4mf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        remove_datatype_from_db(did,"CNAF_TAPE2_USERDISK")
        did = "xnt_"+run+":raw_records_aqmon-rfzvpzj4mf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        remove_datatype_from_db(did,"CNAF_TAPE2_USERDISK")
        did = "xnt_"+run+":raw_records_mv-rfzvpzj4mf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        remove_datatype_from_db(did,"CNAF_TAPE2_USERDISK")
        did = "xnt_"+run+":raw_records_nv-rfzvpzj4mf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        remove_datatype_from_db(did,"CNAF_TAPE2_USERDISK")
        did = "xnt_"+run+":raw_records_aqmon_nv-rfzvpzj4mf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        remove_datatype_from_db(did,"CNAF_TAPE2_USERDISK")

        did = "xnt_"+run+":records-jxkqp76kam"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
        did = "xnt_"+run+":records_he-5mepav2zzf"
        print(did)
        remove_datatype_from_db(did,"CCIN2P3_USERDISK")
        remove_datatype_from_db(did,"NIKHEF_USERDISK")
    """
    
#    add_rule_run_dtype(10658,"peak_basics","UC_DALI_USERDISK","UC_OSG_USERDISK",3)
#    add_rule_run_dtype(10658,"peak_basics","LNGS_USERDISK","UC_DALI_USERDISK",3)

    """
#    remove_datatype_from_db_and_rule(10301,"peaklets_he","NIKHEF_USERDISK")
    #runs = [ 10301, 10302, 10303, 10304, 10305, 10321, 10322, 10323, 10324, 10325, 10326, 10327]
    runs = [ 10328]
    norecords_types = ["lone_hits", "peak_basics", "pulse_counts", "peaklets", "merged_s2s", "peaklet_classification", "peaklet_classification_he", "veto_regions", "pulse_counts_he", "peaklets_he","led_calibration"]
    raw_records_types = ["raw_records", "raw_records_he", "raw_records_aqmon", "raw_records_mv", "raw_records_nv", "raw_records_aqmon_nv"]
    records_types = ["records","records_he"]

    custom_types = ["raw_records_he", "raw_records_aqmon", "raw_records_mv", "raw_records_nv", "raw_records_aqmon_nv"]

    for run in runs:
        for dtype in norecords_types:
#        for dtype in custom_types:
            print(run,dtype)
            #rse = "NIKHEF_USERDISK"
            remove_datatype_from_db_and_rule(run,dtype,"NIKHEF_USERDISK")
#            remove_datatype_from_db_and_rule(run,dtype,"CNAF_TAPE2_USERDISK")
#            remove_datatype_from_db_and_rule(run,dtype,"CCIN2P3_USERDISK")
#            add_rule_run_dtype(run,dtype,"UC_DALI_USERDISK","NIKHEF2_USERDISK",3)
#        for dtype in records_types+raw_records_types:
#            add_rule_run_dtype(run,dtype,"LNGS_USERDISK","NIKHEF2_USERDISK",3)
#            add_rule_run_dtype(run,dtype,"NIKHEF2_USERDISK","CCIN2P3_USERDISK",3)
#            if dtype in raw_records_types:
#                add_rule_run_dtype(run,dtype,"NIKHEF2_USERDISK","CNAF_TAPE2_USERDISK",3)
    """


#    change_status(10090,"eb_ready_to_upload")
#    change_status(10088,"eb_ready_to_upload")
#    change_status(10089,"eb_ready_to_upload")
#    change_status(10092,"eb_ready_to_upload")
#    change_status(10093,"eb_ready_to_upload")
#    change_status(10066,"eb_ready_to_upload")

#    reset_upload("xnt_010427:raw_records-rfzvpzj4mf")
#    reset_upload("xnt_010427:records-jxkqp76kam")
#    reset_upload("xnt_010427:merged_s2s-vukfnvrgnj")
#    reset_upload("xnt_010465:raw_records_aqmon_nv-rfzvpzj4mf")

#    reset_upload("xnt_010520:raw_records-rfzvpzj4mf")
    reset_upload("xnt_010659:peaklet_classification_he-c6h5hdbtcv")

#    reset_upload("xnt_010427:raw_records_mv-rfzvpzj4mf")
#    reset_upload("xnt_010427:raw_records_nv-rfzvpzj4mf")
#    reset_upload("xnt_010427:raw_records_aqmon_nv-rfzvpzj4mf")
#    reset_upload("xnt_010427:records-jxkqp76kam")
#    reset_upload("")
#    reset_upload("")
#    reset_upload("")
    

    """
    reset_upload("")
    reset_upload("")
    reset_upload("")
    reset_upload("")
    reset_upload("")
    reset_upload("")
    reset_upload("")
    """

#    reset_upload("xnt_010380:raw_records-rfzvpzj4mf")
#    reset_upload("xnt_010380:led_calibration-lsdigsccxn")
#    reset_upload("xnt_010380:peaklets_he-2bwh4goml5")
#    reset_upload("xnt_010402:raw_records_aqmon-rfzvpzj4mf")
#    reset_upload("xnt_010402:records_he-5mepav2zzf")
#    reset_upload("xnt_010402:raw_records-rfzvpzj4mf")
#    reset_upload("xnt_010402:raw_records_he-rfzvpzj4mf")

#    reset_upload("xnt_010277:peaklets_he-2bwh4goml5")
#    add_rule("xnt_010301:raw_records_aqmon_nv-rfzvpzj4mf","LNGS_USERDISK","NIKHEF2_USERDISK",3)
#    change_status(9678,"eb_ready_to_upload")
#    change_status(9614,"eb_ready_to_upload")
#    change_status(9615,"eb_ready_to_upload")
#    change_status(9492,"transferred")
#    change_status(8896,"uploading")
#    change_status(7151,"")
#    fixdb(8650)
#    dummy()
#    showcontexts()
