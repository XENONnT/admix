import os
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

def showrun(arg_number,arg_to,arg_dtypes,arg_compact,arg_dumpjson,arg_status,arg_latest):

    #Define data types
    NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
    RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
    RECORDS_DTYPES = helper.get_hostconfig()['records_types']

    #Get other parameters
    DATADIR = helper.get_hostconfig()['path_data_to_upload']
    periodic_check = helper.get_hostconfig()['upload_periodic_check']
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


    data_types = RAW_RECORDS_DTYPES + RECORDS_DTYPES + NORECORDS_DTYPES

    if arg_number == -1 and arg_latest == 0:
        arg_latest = 5
        arg_compact = True

    if arg_latest > 0:
        cursor = db.db.find({}).sort('number',pymongo.DESCENDING).limit(1)
        cursor = list(cursor)
        arg_to = cursor[0]['number']
        arg_number = arg_to - arg_latest + 1
        print('Processing latest {0} runs'.format(arg_latest))            

    if arg_to>arg_number:
        cursor = db.db.find({
            'number': {'$gte': arg_number, '$lte': arg_to}
        }).sort('number',pymongo.ASCENDING)
        print('Runs that will be processed are from {0} to {1}'.format(arg_number,arg_to))
    else:
        cursor = db.db.find({
            'number': arg_number
        })
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
                end_time = run['end'].replace(tzinfo=timezone.utc)
                duration = end_time-start_time
                print("Duration: ",duration)
        
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

            if len(arg_dtypes)>0:
                if dtype not in arg_dtypes:
                    continue

            # Data type name
            print('{0}'.format(dtype))

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



    

def main():
    parser = ArgumentParser("admix-showrun")

    config = Config()

    parser.add_argument("number", type=int, nargs='?', help="Run number to show", default=-1)
    parser.add_argument("--dtypes", nargs="*", help="Restricts infos on the given data types")
    parser.add_argument("--to", type=int, help="Shows runs from the run number up to this value", default=0)
    parser.add_argument("--compact", help="Just list few DB infos as run number, status, date, comments", action='store_true')
    parser.add_argument("--status", help="Just list the run name and its global status", action='store_true')
    parser.add_argument("--json", help="Dumps the whole DB rundoc in pretty style", action='store_true')
    parser.add_argument("--latest", type=int, help="Shows latest runs", default=0)

    args = parser.parse_args()

    if args.dtypes:
        dtypes = args.dtypes
    else:
        dtypes = []

    helper.make_global("admix_config", os.path.abspath(config.get('Admix','config_file')))

    try:
        showrun(args.number,args.to,dtypes,args.compact,args.json,args.status,args.latest)
    except KeyboardInterrupt:
        return 0



