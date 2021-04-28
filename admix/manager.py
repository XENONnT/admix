"""
Common data management functions
"""

from utilix import DB
from admix import rucio

db = DB()


def has_metadata(did):
    scope, dset = did.split(':')
    files = rucio.list_files(did)
    metadata_file = f"{dset}-metadata.json"
    return metadata_file in files


def synchronize(run_number):
    db_data = db.get_data(run_number, host='rucio-catalogue')
    db_datasets = [d['did'].split(':')[1] for d in db_data]

    scope = f'xnt_{run_number:06d}'
    rucio_datasets = rucio.list_datasets(scope)

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
                status = 'stuck'
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
            print(f"Adding {new_datum['did']} at {new_datum['location']}")
            db.update_data(run_number, new_datum)

    # now modify/remove runDB entries according to rucio
    for dset in db_datasets:

        did = f"{scope}:{dset}"

        copies = [d for d in db_data if d['did'] == did]

        base_dict = copies[0].copy()
        base_dict['location'] = None
        base_dict['status'] = None

        db_rses = [d['location'] for d in copies]
        rucio_rules = rucio.list_rules(did)
        rucio_rses = [r['rse_expression'] for r in rucio_rules]

        rses_rm_from_db = set(db_rses) - set(rucio_rses)
        rses_add_to_db = set(rucio_rses) - set(db_rses)
        rses_common = set(db_rses) & set(rucio_rses)

        for rse in rses_rm_from_db:
            pull_data = [d for d in copies if d['location'] == rse][0]
            print(f"Removing db {pull_data['did']} at {pull_data['location']}")
            db.delete_data(run_number, pull_data)

        for rse in rses_add_to_db:
            rule = [r for r in rucio_rules if r['rse_expression'] == rse][0]
            if rule['state'] == 'OK':
                status = 'transferred'
            elif rule['state'] == 'REPLICATING':
                status = 'transferring'
            elif rule['state'] == 'STUCK':
                status = 'stuck'
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
            print(f"Adding {new_datum['did']} at {new_datum['location']}")
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
            else:
                raise ValueError(f"rule {rule['id']} is in an unknown state: {rule['state']}")

            # if state is OK, do one last sanity check that the metadata is there
            # this is edge case I found during outsource once
            if status == 'transferred':
                if not has_metadata(did):
                    status = 'transferring'

            if db_datum['status'] != status:
                updatum = db_datum.copy()
                updatum['status'] = status
                print(f"updating {updatum['did']} at {updatum['location']}")
                db.update_data(run_number, updatum)



def add_rucio_protocol(run_number):
    """Straxen requires for some reason a field protocol=rucio in the data dict"""
    data = db.get_data(run_number, host='rucio-catalogue')
    for d in data:
        if d.get('protocol', 'no_protocol') != 'rucio':
            updatum = d.copy()
            updatum['protocol'] = 'rucio'
            print(f"Updating {d['did']} at {d['location']}")
            db.update_data(run_number, updatum)
