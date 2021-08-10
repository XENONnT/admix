from ..utils import RAW_DTYPES, make_did
from ..uploader import upload
from .daemon import AdmixDaemon


class UploadDaemon(AdmixDaemon):
    # TODO fix this, just a guess
    query = {'status': 'eb_ready_to_upload', 'bootstrax.state': 'done'}

    projection = {'number': 1, 'data': 1, 'bootstrax': 1, 'tags': 1}
    rse = 'LNGS_USERDISK'

    def do_task(self, rundoc):
        # get the datatypes that are on EB machines
        for data_doc in rundoc['data']:
            if 'eb' in data_doc['host']:
                # get did for unique identifier
                did = make_did(rundoc['run_number'], data_doc['type'], data_doc['meta']['lineage_hash'])

                # check if this did is in rucio
                already_uploaded = False
                for other_data_doc in rundoc['data']:
                    if other_data_doc['host'] == 'rucio-catalogue' and other_data_doc['did']==did:
                        already_uploaded = True

                # upload if needed
                if not already_uploaded:
                    self.do_upload(rundoc['number'], data_doc)


    def do_upload(self, run_number, data_doc):
        """Given a data_doc of where some data is on EBs, upload to rucio"""
        # TODO
        # get path of data to upload
        path = data_doc['location']
        rse = self.rse
        did = make_did(run_number, data_doc['type'], data_doc['meta']['lineage_hash'])

        # THIS DOESNT WORK YET, JUST DRAFTING SOME STUFF
        # FIXME -- need to modify upload function to automatically update runDB
        upload(path, rse, did=did, update_db=True)
