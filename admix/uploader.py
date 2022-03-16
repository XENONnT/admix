import os.path
from rucio.common.exception import Duplicate, DataIdentifierNotFound
from .rucio import add_scope, list_files, build_data_dict
from .utils import parse_did, db
from . import clients


def get_default_scope():
    return 'user.' + clients.upload_client.client.account


# TODO could we make this use multithreading or multiprocessing to speed things up?
def upload(path, rse, did=None, lifetime=None, update_db=False):

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

        for missing_file in set(local_files) - set(existing_files):
            _path = os.path.join(path, missing_file)
            to_upload.append(dict(path=_path,
                                  rse=rse,
                                  did_scope=scope,
                                  did_name=missing_file,
                                  dataset_scope=scope,
                                  dataset_name=name,
                                  register_after_upload=True,
                                  lifetime=lifetime
                                  )
                             )
    else:
        upload_dict = dict(path=path,
                           rse=rse,
                           did_scope=scope,
                           dataset_scope=scope,
                           dataset_name=name,
                           register_after_upload=True,
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
        clients.upload_client.upload(to_upload)
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

    return did
