"""
Common data management functions
"""
import os
import shutil
import tempfile

from packaging import version
from tqdm import tqdm
from rucio.common.exception import DataIdentifierNotFound

import admix.rucio
from . import rucio, logger
from . import utils
from .utils import db


def has_metadata(did):
    scope, dset = did.split(':')
    files = rucio.list_files(did)
    metadata_file = f"{dset}-metadata.json"
    return metadata_file in files


def synchronize(run_number, dtype=None):
    # use dtype a lot below so set the arg to _dtype
    _dtype = dtype
    del dtype
    if _dtype is not None:
        db_data = db.get_data(run_number, host='rucio-catalogue', type=_dtype)
    else:
        db_data = db.get_data(run_number, host='rucio-catalogue')

    try:
        db_datasets = list(set([d['did'].split(':')[1] for d in db_data]))
    except KeyError:
        db_datasets = []

    scope = f'xnt_{run_number:06d}'
    rucio_datasets = rucio.list_datasets(scope)
    if _dtype is not None:
        rucio_datasets = [ds for ds in rucio_datasets if ds.split('-')[0] == _dtype]

    # find datasets that are in rucio but not in DB
    missing_db = set(rucio_datasets) - set(db_datasets)

    # add new datasets to runDB that are missing
    for dset in missing_db:
        did = f"{scope}:{dset}"

        dtype, strax_hash = dset.split('-')

        # get the locations of this dataset
        rules = rucio.list_rules(did)

        for rule in rules:
            if rule['state'] == 'OK':
                status = 'transferred'
            elif rule['state'] == 'REPLICATING':
                status = 'transferring'
            elif rule['state'] == 'STUCK':
                status = 'error'
            elif rule['state'] == 'SUSPENDED':
                status = 'error'
            else:
                raise ValueError(f"rule {rule['id']} is in an unknown state: {rule['state']}")

            location = rule['rse_expression']

            # if state is OK, do one last sanity check that the metadata is there
            # this is edge case I found during outsource once
            if status == 'transferred':
                if not has_metadata(did):
                    status = 'transferring'

            new_datum = dict(host='rucio-catalogue',
                             did=did,
                             type=dtype,
                             status=status,
                             location=location,
                             protocol='rucio'
                             )
            logger.debug(f"Adding {new_datum['did']} at {new_datum['location']}")
            db.update_data(run_number, new_datum)

    # now modify/remove runDB entries according to rucio
    for dset in db_datasets:
        did = f"{scope}:{dset}"

        copies = [d for d in db_data if d['did'] == did]

        base_dict = copies[0].copy()
        base_dict['location'] = None
        base_dict['status'] = None

        db_rses = [d['location'] for d in copies]
        try:
            rucio_rules = rucio.list_rules(did)
            rucio_rses = [r['rse_expression'] for r in rucio_rules]
        except DataIdentifierNotFound:
            rucio_rses = []

        # check for dupicate entries in the runDB
        duplicated_rses = []
        if len(db_rses) != len(set(db_rses)):
            duplicated_rses = list(set([rse for rse in db_rses if db_rses.count(rse) > 1]))

        rses_rm_from_db = set(db_rses) - set(rucio_rses)
        rses_add_to_db = set(rucio_rses) - set(db_rses)
        rses_common = set(db_rses) & set(rucio_rses)

        for rse in rses_rm_from_db:
            pull_data = [d for d in copies if d['location'] == rse][0]
            logger.debug(f"Removing db {pull_data['did']} at {pull_data['location']}")
            db.delete_data(run_number, pull_data)

        for rse in rses_add_to_db:
            rule = [r for r in rucio_rules if r['rse_expression'] == rse][0]
            if rule['state'] == 'OK':
                status = 'transferred'
            elif rule['state'] == 'REPLICATING':
                status = 'transferring'
            elif rule['state'] == 'STUCK':
                status = 'stuck'
            elif rule['state'] == 'SUSPENDED':
                status = 'error'
            else:
                raise ValueError(f"rule {rule['id']} is in an unknown state: {rule['state']}")

            # if state is OK, do one last sanity check that the metadata is there
            # this is edge case I found during outsource once
            if status == 'transferred':
                if not has_metadata(did):
                    status = 'transferring'

            new_datum = base_dict.copy()
            new_datum['location'] = rse
            new_datum['status'] = status
            logger.debug(f"Adding {new_datum['did']} at {new_datum['location']}")
            db.update_data(run_number, new_datum)

        for rse in rses_common:
            db_datum = [d for d in copies if d['location'] == rse][0]

            rule = [r for r in rucio_rules if r['rse_expression'] == rse][0]

            if rule['state'] == 'OK':
                status = 'transferred'
            elif rule['state'] == 'REPLICATING':
                status = 'transferring'
            elif rule['state'] == 'STUCK':
                status = 'stuck'
            elif rule['state'] == 'SUSPENDED':
                status = 'error'
            else:
                raise ValueError(f"rule {rule['id']} is in an unknown state: {rule['state']}")

            # if state is OK, do one last sanity check that the metadata is there
            # this is edge case I found during outsource once
            if status == 'transferred':
                if not has_metadata(did):
                    logger.debug(f"Warning! no metadata found for {did}")
                    status = 'transferring'

            if db_datum['status'] != status:
                updatum = db_datum.copy()
                updatum['status'] = status
                logger.debug(f"Updating {updatum['did']} at {updatum['location']}")
                db.update_data(run_number, updatum)

        if len(duplicated_rses):
            for rse in duplicated_rses:
                logger.debug(f"We have duplicate entries for {did} at {rse}.")
                # let's just remove all of them and then add back in
                ddocs = [ddoc for ddoc in copies if ddoc['location'] == rse]

                for ddoc in ddocs:
                    db.delete_data(run_number, ddoc)

            synchronize(run_number)


def add_rucio_protocol(run_number):
    """Straxen requires for some reason a field protocol=rucio in the data dict"""
    data = db.get_data(run_number, host='rucio-catalogue')
    for d in data:
        if d.get('protocol', 'no_protocol') != 'rucio':
            updatum = d.copy()
            updatum['protocol'] = 'rucio'
            logger.debug(f"Updating {d['did']} at {d['location']}")
            db.update_data(run_number, updatum)


def get_outdated_strax_info(not_outdated_version,
                            return_current_hashes=False
                            ):
    coll = utils.xent_context_collection

    # get versions of all straxen versions before some given 'good' version
    thresh_version = version.parse(not_outdated_version)

    cursor = list(coll.find({}, {'straxen_version': 1}))
    versions = set([version.parse(d['straxen_version']) for d in cursor])
    outdated_versions = set(sorted([v for v in versions if v < thresh_version], reverse=True))
    save_versions = versions - outdated_versions
    # if no save_versions, then save the data of the latest version
    if not len(save_versions):
        v = sorted(list(versions))[-1]
        save_versions = [v]
        outdated_versions.remove(v)

    # get the hashes of all datatypes we want to save
    save_hashes = dict()
    for v in save_versions:
        cursor = coll.find({'straxen_version': v.public})
        for doc in cursor:
            hashes = doc['hashes']
            for dtype, h in hashes.items():
                if dtype in save_hashes:
                    if h not in save_hashes[dtype]:
                        save_hashes[dtype].append(h)
                else:
                    save_hashes[dtype] = [h]

    delete_hashes = dict()
    for v in outdated_versions:
        cursor = coll.find({'straxen_version': v.public})
        for doc in cursor:
            hashes = doc['hashes']
            for dtype, h in hashes.items():
                if dtype in save_hashes and h in save_hashes[dtype]:
                    pass
                else:
                    if dtype in delete_hashes:
                        if h not in delete_hashes[dtype]:
                            delete_hashes[dtype].append(h)
                    else:
                        delete_hashes[dtype] = [h]

    # just to double check
    for dtype, h in delete_hashes.items():
        if dtype in save_hashes:
            assert h not in save_hashes[dtype]
    if return_current_hashes:
        return delete_hashes, save_hashes
    return delete_hashes


def find_outdated_data(max_straxen_version, specific_dtype=None):
    def get_dids(ddoc, dtype, hash_list):
        ret = []
        for d in ddoc:
            if d['type'] == dtype and d.get('did'):
                ddoc_hash = d['did'].split(':')[1].split('-')[1]
                if ddoc_hash in hash_list:
                    if d['did'] not in ret:
                        ret.append(d['did'])
        if not len(ret):
            raise RuntimeError(f"No dids found for {dtype}")
        return ret

    outdated_info = get_outdated_strax_info(max_straxen_version)
    if specific_dtype:
        if isinstance(specific_dtype, str):
            outdated_info = {specific_dtype: outdated_info[specific_dtype]}
        else:
            outdated_info = {key: val for key, val in outdated_info.items() if key in specific_dtype}

    outdated_dids = dict()
    for dtype, hsh_list in tqdm(outdated_info.items(), desc='Finding data we can delete'):
        query = {'$or': [{'data': {'$elemMatch': {'type': dtype,
                                                  'did': {'$regex': h}}}}
                         for h in hsh_list]
                 }
        ndocs = utils.xent_runs_collection.count_documents(query)
        if ndocs > 0:
            cursor = utils.xent_runs_collection.find(query, {'number': 1, 'data': 1})
            if dtype not in outdated_dids:
                outdated_dids[dtype] = list()
            for run in cursor:
                dids = get_dids(run['data'], dtype, hsh_list)
                outdated_dids[dtype].extend(dids)
    return outdated_dids


def copy_high_level_data(runlist, move_dont_copy=False):
    """Copy/move all high level data for a given runlist"""
    pass


@rucio.requires_production
def containerize(run_number, straxen_version, context='xenonnt_online',
                 rse=None):
    # get the dtype-hash pairs for this context and this straxen version
    hashes = db.get_context(context, straxen_version)['hashes']

    # get the data entries
    data = db.get_data(run_number, host='rucio-catalogue')

    # loop over data entries, find high-level  ones, and add to a list
    # this list will be used to create the new container
    high_level_dids = []
    for d in data:
        if d['type'] not in utils.RAWish_DTYPES:
            if hashes.get(d['type']) in d.get('did', ''):
                if d['did'] not in high_level_dids:
                    high_level_dids.append(d['did'])

    # if there are not any high-level DIDs, nothing to do
    if not len(high_level_dids):
        print(f"No high-level DIDs found for run {run_number}, straxen {straxen_version}, context {context}")
        return

    container_did = utils.make_highlevel_container_did(run_number, straxen_version)

    # if container does not exist, make it
    scope, container_name = container_did.split(':')
    existing_containers = rucio.list_containers(scope)
    if container_name not in existing_containers:
        print(f"Creating container {container_did}")
        rucio.add_container(scope, container_name)

    # now loop over highlevel datasets, and add non-existing ones to the container
    existing_content = rucio.list_content(container_did)

    to_attach = []
    for dset_did in high_level_dids:
        if dset_did not in existing_content:
            to_attach.append(dset_did)

    if len(to_attach):
        print("Attaching", to_attach)
        rucio.attach(container_did, to_attach, rse=rse)
    else:
        print(f"Nothing to attach for run {run_number}, straxen {straxen_version}, context {context}")


def clean_local_dir(path, before_straxen_version, ensure_rucio=False, dry_run=True):

    """Checks a local directory for any outdated straxen version and deletes it.

    :param: path: Local data path to check for outdated data.
    :param: before_straxen_version: Delete any data OLDER than this version. This version will not be deleted.
    :param: ensure_rucio: check if data is uploaded to rucio before deleting.
    :param: dry_run: When True, do not delete anything -- just compile a list of the datasets that can be deleted and write to stdout.
    """

    # get context names from the runDB
    cursor = utils.xent_context_collection.find({}, {'name': 1})
    contexts = set([c['name'] for c in cursor if c.get('name')])

    save_hashes = {}
    for context in contexts:
        _delete_hashes, _save_hashes = get_outdated_strax_info(before_straxen_version,
                                                               context=context,
                                                               return_current_hashes=True)
        for dtype, hash_list in _save_hashes.items():
            if dtype in save_hashes:
                save_hashes[dtype] = save_hashes[dtype] | set(hash_list)

            else:
                save_hashes[dtype] = set(hash_list)


    # only used for when dry_run = True
    to_delete = []
    desc = f"Finding outdated data in {path}" if dry_run else f"Deleting outdated data in {path}"
    for dataset in tqdm(os.listdir(path), desc=desc):
        number, dtype, lineage_hash = utils.parse_dirname(dataset)
        if not save_hashes.get(dtype) or lineage_hash in save_hashes[dtype]:
            # if dtype not in save_hashes, assume it is from another context (so dont delete)
            # if is in save_hashes but the specific hash is in a version to save, dont delete
            continue
        # if we get here, then this dtype-hash pair can be deleted in principle
        if ensure_rucio:
            # check that the data is in rucio first
            # do direct rucio query lest we rely on runDB-rucio synchronization
            did = utils.make_did(number, dtype, lineage_hash)
            try:
                rules = admix.rucio.get_rses(did, state="OK")
                if not len(rules):
                    continue
            except DataIdentifierNotFound:
                continue

        if dry_run:
            # dont delete now, just add to a running list we will output later
            to_delete.append(dataset)
        else:
            shutil.rmtree(os.path.join(path, dataset))

    if dry_run:
        _, tmpfile = tempfile.mkstemp(suffix='.txt', dir="./")
        with open(tmpfile, 'w') as f:
            for dataset in to_delete:
                f.write(f"{os.path.join(path, dataset)}\n")
        print(f"Files that we can delete written to {tmpfile}")


