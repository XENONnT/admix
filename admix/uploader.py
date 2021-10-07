import os.path

from rucio.client.uploadclient import UploadClient
from rucio.common.exception import Duplicate

from .rucio import add_scope, list_files


upload_client = UploadClient()


def get_default_scope():
    return 'user.' + upload_client.client.account


# TODO automatically update RunDB
# TODO could we make this use multithreading or multiprocessing to speed things up?
def upload(path, rse, did=None, check_existing=True, lifetime=None,
           update_db=False):

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
        add_scope(upload_client.client.account, scope)
    except Duplicate:
        pass

    if check_existing:
        # compare files attached to this DID to that in path, and only upload missing ones
        if os.path.isdir(path):
            existing_files = list_files(did)
            local_files = os.listdir(path)
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
            raise ValueError("Can't pass check_existing for a single DID/file")

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

    if len(to_upload):
        return upload_client.upload(to_upload)
    else:
        print(f"Nothing to upload at {path}")
