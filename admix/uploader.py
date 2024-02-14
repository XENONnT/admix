import os.path
from rucio.common.exception import Duplicate, DataIdentifierNotFound
from .rucio import add_scope, list_files, get_rule
from .utils import parse_did, db
from . import clients



def get_default_scope():
    return 'user.' + clients.upload_client.client.account

def preupload(path, rse, did):
    """
    A function supposed to be run before upload to avoid orphan files failing the upload. 
    It does the following
        - It adds the dataset associated to the did we wanted to upload
        - It loops over all local files to be uploaded, so to know their number and their names. 
          For each file, it searches in Rucio catalogue if such a filename is already present. 
          If so, it attaches it to the dataset
        - Finally, it creates a replication rule on the RSE (the RSE is an input parameter of the 
          preupload function, however, it's important that the RSE must be the same chosen by the 
          previous upload attempt). After this latest operation, the did will show up in Rucio.
    """
    if not os.path.isdir(path):
        return

    local_files = os.listdir(path)
    nfiles = len(local_files)
    scope, name = did.split(':')
    try:
        clients.did_client.add_dataset(scope,name)
    except:
        print("DID {0} already exists".format(did))
    for local_file in local_files:
        try:
            clients.did_client.attach_dids(scope,name,[{'scope':scope,'name':local_file}])
        except:
            print("File {0} could not be attached".format(local_file))
    try:
        clients.rule_client.add_replication_rule([{'scope':scope,'name':name}],1,rse)
    except:
        print("The rule for DID {0} already exists".format(did))

# TODO could we make this use multithreading or multiprocessing to speed things up?
def upload(path, rse, 
           register_after_upload=False, verbose=True, did=None, lifetime=None, update_db=False):

    # set scope initially to default one. will overwrite it below if did passed
    scope = get_default_scope()

    if did:
        if ':' in did:
            scope, name = did.split(':')
        else:
            name = did
    else:
        name = os.path.basename(path)
        did = f"{scope}:{name}"

    try:
        add_scope(clients.upload_client.client.account, scope)
    except Duplicate:
        pass

    # compare files attached to this DID to that in path, and only upload missing ones
    if os.path.isdir(path):
        existing_files = []
        local_files = os.listdir(path)
        try:
            existing_files = list_files(did)
        except DataIdentifierNotFound:
            pass

        to_upload = []

        missing_files = set(local_files) - set(existing_files)
        if verbose:
            print(f"Found {len(missing_files)} files to upload:")
            print(missing_files)
            print("------")
        for missing_file in missing_files:
            _path = os.path.join(path, missing_file)
            to_upload.append(dict(path=_path,
                                  rse=rse,
                                  did_scope=scope,
                                  did_name=missing_file,
                                  dataset_scope=scope,
                                  dataset_name=name,
                                  register_after_upload=register_after_upload,
                                  lifetime=lifetime
                                  )
                             )
    else:
        upload_dict = dict(path=path,
                           rse=rse,
                           did_scope=scope,
                           dataset_scope=scope,
                           dataset_name=name,
                           register_after_upload=register_after_upload,
                           lifetime=lifetime
                           )
        to_upload = [upload_dict]

    # get data dict to add to database
    number, dtype, lineage_hash = parse_did(did)
    data_dict = dict(did=did,
                     type=dtype,
                     location=rse,
                     status='transferring',
                     host='rucio-catalogue',
                     meta=dict(),
                     protocol='rucio',
                    )

    if len(to_upload):
        if update_db:
            db.update_data(number, data_dict)
        try:
            clients.upload_client.upload(to_upload)
        except Exception as e:
            if verbose:
                print(f"Upload failed for {path}")
                print(e)
            return did
        # then update db again when complete
        if update_db:
            data_dict['status'] = 'transferred'
            # get files, size etc
            files = list_files(did, verbose=True)
            size = sum([f['bytes'] for f in files]) / 1e6
            data_dict['meta'] = dict(lineage_hash=lineage_hash,
                                     size_mb=size,
                                     file_count=len(files)
                                     )
            db.update_data(number, data_dict)
    else:
        print(f"Nothing to upload at {path}")

    try:
        if not get_rule(did,rse):
            clients.rule_client.add_replication_rule(dids=[{'scope':scope,'name':name}],copies=1,rse_expression=rse)
            print(f"Missing rule has been added")
    except Exception as e:
        if verbose:
            print(f"Insertion of rule failed")
            print(e)
        return did

        
    return did
