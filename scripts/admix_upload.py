import os
import time
import shutil
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.keyword import Keyword
from admix.interfaces.database import ConnectMongoDB
from admix.utils import make_did

DB = ConnectMongoDB()

DTYPES = ['raw_records', 'raw_records_he', 'raw_records_aqmon', 'raw_records_mv']
DATADIR = '/eb/ebdata'


def find_new_data():
    """"""
    query = {'number': {'$gte': 7150},
             "status": {"$exists": False},
             "bootstrax.state": "done",
             }

    cursor = DB.db.find(query, {'number': 1})

    for r in cursor:
        DB.SetStatus(r['number'], 'eb_ready_to_upload')


def find_data_to_upload():
    cursor = DB.db.find({'status': 'eb_ready_to_upload'}, {'number': 1, 'data': 1})
    ids = []

    for r in cursor:
        dtypes = set([d['type'] for d in r['data']])
        # check if all of the necessary data types are in the database
        if set(DTYPES) <= dtypes:
            ids.append(r['_id'])
    return ids


def do_upload(periodic_check=300):
    #rc_reader_path = "/home/datamanager/software/admix/admix/config/xenonnt_format.config"
    #rc_reader = ConfigRucioDataFormat()
    #rc_reader.Config(rc_reader_path)

    rc = RucioSummoner()

    # get the data to upload
    ids_to_upload = find_data_to_upload()

    cursor = DB.db.find({'_id': {"$in": ids_to_upload},
                         #'number': 7157
                         },
                        {'number': 1, 'data': 1, 'dids': 1})

    cursor = list(cursor)

    # check transfers
    check_transfers()
    last_check = time.time()

    for run in cursor:
        number = run['number']
        print(f"\n\nUploading run {number}")
        for dtype in DTYPES:
            print(f"\t==> Uploading {dtype}")
            # get the datum for this datatype
            datum = None
            in_rucio = False
            for d in run['data']:
                if d['type'] == dtype and 'eb' in d['host']:
                    datum = d

                if d['type'] == dtype and d['host'] == 'rucio-catalogue':
                    in_rucio = True

            if datum is None:
                print(f"Data type {dtype} not found for run {number}")
                continue

            file = datum['location'].split('/')[-1]


            hash = file.split('-')[-1]

            upload_path = os.path.join(DATADIR, file)

            # create a DID to upload
            did = make_did(number, dtype, hash)

            # check if a rule already exists for this DID on LNGS
            rucio_rule = rc.GetRule(upload_structure=did, rse="LNGS_USERDISK")

            # if not in rucio already and no rule exists, upload into rucio
            if not in_rucio and not rucio_rule['exists']:
                result = rc.Upload(did,
                                   upload_path,
                                   'LNGS_USERDISK',
                                   lifetime=None)

                print("Dataset uploaded.")

            # if upload was successful, tell runDB
            rucio_rule = rc.GetRule(upload_structure=did, rse="LNGS_USERDISK")
            data_dict = {'host': "rucio-catalogue",
                         'type': dtype,
                         'location': 'LNGS_USERDISK',
                         'lifetime': rucio_rule['expires'],
                         'status': 'transferred',
                         'did': did,
                         'protocol': 'rucio'
                         }

            if rucio_rule['state'] == 'OK':
                if not in_rucio:
                    DB.AddDatafield(run['_id'], data_dict)

                # add a DID list that's easy to query by DB.GetDid
                # check if did field exists yet or not
                # if not run.get('dids'):
                #     DB.db.find_one_and_update({'_id': run['_id']},
                #                               {'$set': {'dids': {dtype: did}}}
                #                               )
                # else:
                #     print("Updating DID list")
                #     DB.db.find_one_and_update({'_id': run['_id']},
                #                               {'$set': {'dids.%s' % dtype: did}}
                #                               )

            # add rule to OSG and Nikhef
            # TODO make this configurable
            for rse in ['UC_OSG_USERDISK']:
                add_rule(number, dtype, rse)

            # finally, delete the eb copy
            #remove_from_eb(number, dtype)

        if time.time() - last_check > periodic_check:
            check_transfers()
            last_check = time.time()


def add_rule(run_number, dtype, hash, rse, lifetime=None, update_db=True):
    did = make_did(run_number, dtype, hash)
    rc = RucioSummoner()
    result = rc.AddRule(did, rse, lifetime=lifetime)
    #if result == 1:
    #   return
    print(f"Rule Added: {did} ---> {rse}")

    if update_db:
        rucio_rule = rc.GetRule(did, rse=rse)
        data_dict = {'host': "rucio-catalogue",
                     'type': dtype,
                     'location': rse,
                     'lifetime': rucio_rule['expires'],
                     'status': 'transferring',
                     'did': did,
                     'protocol': 'rucio'
                     }
        DB.db.find_one_and_update({'number': run_number},
                                  {'$set': {'status': 'transferring'}}
                                  )

        docid = DB.db.find_one({'number': run_number}, {'_id': 1})['_id']
        DB.AddDatafield(docid, data_dict)


def check_transfers():
    cursor = DB.db.find({'status': 'transferring'}, {'number': 1, 'data': 1})

    rc = RucioSummoner()

    cursor = list(cursor)

    print("Checking transfer status of %d runs" % len(cursor))


    for run in list(cursor):
        # for each run, check the status of all REPLICATING rules
        rucio_stati = []
        for d in run['data']:
            if d['host'] == 'rucio-catalogue':
                if d['status'] != 'transferring':
                    rucio_stati.append(d['status'])
                else:
                    did = d['did']
                    status = rc.CheckRule(did, d['location'])
                    if status == 'REPLICATING':
                        rucio_stati.append('transferring')
                    elif status == 'OK':
                        # update database
                        print("Updating DB for run %d, dtype %s" % (run['number'], d['type']))
                        DB.db.find_one_and_update({'_id': run['_id'],'data': {'$elemMatch': d}},
                                                  {'$set': {'data.$.status': 'transferred'}}
                                                  )
                        rucio_stati.append('transferred')

                    elif status == 'STUCK':
                        DB.db.find_one_and_update({'_id': run['_id'], 'data': {'$elemMatch': d}},
                                                  {'$set': {'data.$.status': 'error'}}
                                                  )
                        rucio_stati.append('error')

        # are there any other rucio rules transferring?
        if len(rucio_stati) > 0 and all([s == 'transferred' for s in rucio_stati]):
            DB.SetStatus(run['number'], 'transferred')


def clear_db():
    dtypes = ['raw_records'] #, 'raw_records_mv', 'raw_records_aqmon', 'raw_records_lowgain']
    numbers = [7155, 7156]
    for run in numbers:
        doc = DB.GetRunByNumber(run)[0]
        docid = doc['_id']

        for d in doc['data']:
            if d['host'] == 'rucio-catalogue' and d['type'] in dtypes:
                DB.RemoveDatafield(docid, d)
                time.sleep(1)

        DB.SetStatus(run, 'eb_ready_to_upload')


def purge():
    query = {'data': {"$elemMatch": {'location': "LNGS_USERDISK"}}}
    cursor = list(DB.db.find(query, {'number': 1, 'data': 1}))

    rc = RucioSummoner()

    print("Checking %d runs if they can be purged from LNGS" % len(cursor))
    for run in cursor:
        print(run['number'])
        # get datatypes that are still at LNGS, according to runDB
        dtypes = set()
        for d in run['data']:
            if d['host'] == 'rucio-catalogue' and d['location'] == 'LNGS_USERDISK':
                dtypes.add(d['type'])

        print(dtypes)
        # now loop over the LNGS dtypes to see if we can purge anything
        # require 2 rucio copies outside LNGS to purge
        lngs_datum = None
        for dtype in dtypes:
            rucio_copies = 0
            for d in run['data']:
                if (d['host'] == 'rucio-catalogue'
                    and d['type'] == dtype
                    and d['status'] == 'transferred'
                    ):

                    if d['location'] == 'LNGS_USERDISK':
                        lngs_datum = d
                    else:
                        rucio_copies += 1



            if rucio_copies >= 2:
                # remove the LNGS rule
                did = DB.GetDid(run['number'], dtype)
                scope, dset = did.split(':')
                rules = rc._rucio.ListDidRules(scope, dset)
                # get the rule id
                rule_id = None
                for r in rules:
                    if r['rse_expression'] == 'LNGS_USERDISK':
                        rule_id = r['id']

                if rule_id is None:
                    print("No LNGS rule found for run %d %s" % (run['number'], dtype))

                else:
                    print("Deleting LNGS rule for %d %s" % (run['number'], dtype))
                    DB.RemoveDatafield(run['_id'], lngs_datum)
                    rc.DeleteRule(rule_id)

                # TEMPORARY: check if there are still copies on eb, if so remove them.
                remove_from_eb(run['number'], dtype)
        break


def remove_from_eb(number, dtype):
    query = {'number': number}
    cursor = DB.db.find_one(query, {'number': 1, 'data': 1})
    ebdict = None
    for d in cursor['data']:
        if '.xenon.local' in d['host'] and d['type'] == dtype:
            ebdict = d

    # get name of file (really directory) of this dtype in eb storage
    if ebdict is None:
        print(f"No eventbuilder datum found for run {number} {dtype} Exiting.")
        return

    file = ebdict['location'].split('/')[-1]
    path_to_rm = os.path.join(DATADIR, file)

    print(path_to_rm)
    print(ebdict)
    shutil.rmtree(path_to_rm)
    #DB.RemoveDatafield(cursor['_id'], ebdict)


def main():
    while True:
        find_new_data()
        print("Starting uploads")
        do_upload()
        print("Sleeping...\n")
        #time.sleep(300)
        break



if __name__ == "__main__":
    #main()
    # clear_db()
    purge()
    #check_transfers()

