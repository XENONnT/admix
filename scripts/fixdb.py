import os
import time
import shutil
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.keyword import Keyword

from admix.utils import make_did



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



if __name__ == "__main__":

    #[8355, 8356, 8357, 8358, 8359, 8360, 8361, 8362, 8363, 8364, 8365, 8366]

    #Indeed up to 8360, data were already uploaded and transferred outside. I need to patch them. 
    #8361 was interrupted during the issue, so for this I will remove everything in Rucio and restart the upload from scratch).
    #Then, I restarted admix in the usual way from 8362 and it is going fine

    fixdb(7887)

