import os
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.keyword import Keyword
from admix.interfaces.database import ConnectMongoDB

DB = ConnectMongoDB()

DTYPES = ['raw_records', 'raw_records_lowgain', 'raw_records_acqmon', 'raw_records_mv']
DATADIR = '/eb/ebdata'

def set_status(docid, status):
    DB.db.find_one_and_update({'_id': docid},
                              {'$set': {'status': status}}
                              )


def find_new_data():
    """"""
    query = {'number': {'$gte': 7157},
             "status": {"$exists": False}
             }

    cursor = DB.db.find(query, {'number': 1})

    for r in cursor:
        set_status(r['_id'], 'needs_upload')


def find_data_to_upload():
    cursor = DB.db.find({'status': 'needs_upload'}, {'number': 1, 'data': 1})
    ids = []

    for r in cursor:
        dtypes = set([d['type'] for d in r['data']])
        # check if all of the necessary data types are in the database
        if set(DTYPES) <= dtypes:
            ids.append(r['_id'])
    return ids


def do_upload():
    rc_reader_path = "/home/datamanager/software/admix/admix/config/xenonnt_format.config"
    rc_reader = ConfigRucioDataFormat()
    rc_reader.Config(rc_reader_path)

    rc = RucioSummoner()

    # get the data to upload
    ids_to_upload = find_data_to_upload()

    cursor = DB.db.find({'_id': {"$in": ids_to_upload}},
                        {'number': 1, 'data': 1}
                        )

    for run in cursor:
        print(f"Uploading run {number}")
        number = run['number']
        for dtype in DTYPES:
            print(f"\t==> Uploading {dtype}")
            # get the datum for this datatype
            datum = None
            for d in run['data']:
                if d['type'] == dtype and 'eb' in d['location']:
                    datum = d

            if datum is None:
                print(f"Data type {dtype} not found for run {number}")
                continue

            file = datum['location'].split('/')[-1]

            #Init a class to handle keyword strings:
            keyw = Keyword()
            hash = file.split('-')[-1]
            rucio_template = rc_reader.GetPlugin(dtype)
            upload_path = os.path.join(DATADIR, file)

            keyw.SetTemplate({'hash': hash, 'plugin': dtype, 'number':'%06d' % number})

            rucio_template = keyw.CompleteTemplate(rucio_template)

            result = rc.Upload(upload_structure=rucio_template,
                               upload_path=upload_path,
                               rse='LNGS_USERDISK',
                               rse_lifetime=None)


def main():
    do_upload()


if __name__ == "__main__":
    find_new_data()
