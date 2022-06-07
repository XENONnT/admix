"""
Contains rucio commands with XENON-specific wrappers
"""
from functools import wraps
import re
from tqdm import tqdm
import rucio.common.exception

import admix.utils
from . import logger
from .utils import parse_did, db, RAW_DTYPES
from . import clients


needs_client = clients.needs_client


def requires_production(func):
    """Decorator used for functions that require the production account credentials"""
    def wrapped(*args, **kwargs):
        if clients.rucio_client.account != 'production':
            raise RucioPermissionError(f"You must be the production user to call {func.__name__}")
        return func(*args, **kwargs)
    return wrapped


def build_data_dict(did, rse, status):
    number, dtype, h = parse_did(did)
    files = list_files(did, verbose=True)
    size = sum([f['bytes'] for f in files]) / 1e6
    data = dict(did=did,
                type=dtype,
                location=rse,
                status=status,
                host='rucio-catalogue',
                meta=dict(lineage_hash=h,
                          size_mb=size,
                          file_count=len(files)
                          ),
                protocol='rucio',
                )
    return data


def update_db(mode):
    """Mode can be 'add', 'move', 'delete'"""
    def decorator(func):
        @wraps(func)
        def wrapped(*args,  **kwargs):
            # did and rse are the first 2 args
            did, rse = args[0], args[1]
            update_db = kwargs.get('update_db', False)
            if not update_db or db is None:
                return func(*args, **kwargs)

            func(did, rse, **kwargs)
            number, dtype, h = parse_did(did)
            if mode == 'add':
                data = build_data_dict(did, rse, status='transferring')
                db.update_data(number, data)

            elif mode == 'delete':
                data = db.get_data(number, type=dtype, did=did, location=rse)
                if len(data) == 0:
                    pass
                else:
                    for d in data:
                        db.delete_data(number, d)

            elif mode == 'move':
                from_rse = kwargs.pop('from_rse')
                if not from_rse:
                    raise ValueError(f"'from_rse' must be passed when calling {func.__name__}")
                from_data = db.get_data(number, type=dtype, did=did, location=from_rse)
                if len(from_data) == 0:
                    to_data = build_data_dict(did, rse, 'transferring')
                else:
                    to_data = from_data[0].copy()
                    to_data['location'] = rse
                    to_data['status'] = 'transferring'
                db.update_data(number, to_data)
                db.delete_data(number, from_data)
        return wrapped
    return decorator


@needs_client
def get_did(did):
    scope, name = did.split(':')
    try:
        return clients.rucio_client.get_did(scope, name)
    except rucio.common.exception.DataIdentifierNotFound:
        pass


@needs_client
def get_did_type(did):
    scope, name = did.split(':')
    return clients.rucio_client.get_did(scope, name)['type']


@needs_client
def list_rules(did, **filters):
    scope, name = did.split(':')
    # check if did is a file or a dataset
    if get_did_type(did) == 'FILE':
        rules = clients.rucio_client.list_associated_rules_for_file(scope, name)

    else:
        rules = clients.rucio_client.list_did_rules(scope, name)
    # get rules that pass some filter(s)
    ret = []
    for rule in rules:
        selected = True
        for key, val in filters.items():
            if rule[key] != val:
                selected = False
        if selected:
            ret.append(rule)
    return ret

@needs_client
def get_rses(did, **filters):
    rules = list_rules(did, **filters)
    rses = [r['rse_expression'] for r in rules]
    return rses

@needs_client
def get_rule(did, rse):
    rules = list_rules(did, rse_expression=rse)
    if len(rules) == 0:
        logger.debug(f"No rule found for {did} at {rse}!")
        return
    return rules[0]


@needs_client
@requires_production
@update_db('add')
def add_rule(did, rse, copies=1, update_db=False, quiet=False, **kwargs):
    scope, name = did.split(':')
    did_dict = dict(scope=scope, name=name)
    try:
        clients.rucio_client.add_replication_rule([did_dict], copies, rse_expression=rse, **kwargs)
    except rucio.common.exception.DuplicateRule:
        if not quiet:
            print(f"Rule already exists for {did} at {rse}")
        return
    if not quiet:
        print(f"Replication rule added for {did} at {rse}")


@needs_client
@requires_production
@update_db('delete')
def delete_rule(did, rse, purge_replicas=True, _careful=True, _required_copies=1, update_db=False,
                quiet=False):
    number, dtype, hsh = parse_did(did)
    if dtype in RAW_DTYPES and _required_copies < 1:
        raise DataPolicyError("You cant remove raw_records data. Shame on you. ")
    rule = get_rule(did, rse)
    if not rule:
        raise RuleNotFoundError(f"No rule found for {did} at {rse}!")
    # make sure there is at least one other copy
    if _careful:
        rules = list_rules(did, state='OK')
        other_rules = [r for r in rules if r['rse_expression'] != rse and
                       r['expires_at'] is None
                       ]
        if len(other_rules) < _required_copies:
            raise DataPolicyError(f"We require at least {_required_copies} long-term copies "
                                  f"and deleting one for {did} would result in {len(other_rules)}."
                                  )
    clients.rucio_client.delete_replication_rule(rule['id'], purge_replicas=purge_replicas)
    if not quiet:
        print(f"Replication rule for {did} at {rse} removed.")


@needs_client
@requires_production
def erase(did, now=False, update_db=False):
    scope, name = did.split(':')
    if get_did_type(did) == "FILE":
        dtype = name.split('-')[0]
    else:
        number, dtype, hsh = parse_did(did)
    if dtype in admix.utils.RAW_DTYPES:
        print(f"You cannot erase {dtype} data. Shame on you")
        return
    # delete DID in 10 seconds if pass now=True, else copy what rucio does and set it to 24 hours.
    # see https://github.com/rucio/rucio/blob/master/bin/rucio#L883
    value = 10 if now else 86400
    if update_db and get_did_type(did) == "DATASET":
        number, dtype, h = parse_did(did)
        data = db.get_data(number, did=did)
        for d in data:
            db.delete_data(number, d)
    try:
        clients.rucio_client.set_metadata(scope, name, key='lifetime', value=value)
    except rucio.common.exception.DataIdentifierNotFound:
        print(f"{did} does not exist")


@requires_production
@update_db('add')
def add_conditional_rule(did, rse, from_rse=None, update_db=False, **kwargs):
    """Convenience wrapper around add_rule"""
    add_rule(did, rse, source_replica_expression=from_rse, **kwargs)


@requires_production
@update_db('move')
def move_rule(did, rse, from_rse, update_db=False):
    pass


@needs_client
def add_scope(account, scope):
    return clients.rucio_client.add_scope(account, scope)


@needs_client
@requires_production
def add_production_scope(scope):
    return add_scope('production', scope)


@needs_client
@requires_production
def add_container(scope, name, **kwargs):
    return clients.rucio_client.add_container(scope, name, **kwargs)


@needs_client
def list_datasets(scope):
    datasets = [d for d in clients.rucio_client.list_dids(scope, {'type': 'dataset'}, type='dataset')]
    return datasets


@needs_client
def list_containers(scope):
    containers = [d for d in clients.rucio_client.list_dids(scope, {'type': 'container'}, type='container')]
    return containers


@needs_client
def list_scopes(regex_pattern='.*'):
    pattern = re.compile(regex_pattern)
    _scopes = clients.rucio_client.list_scopes()
    scopes = [s for s in _scopes if pattern.match(s)]
    return scopes


@needs_client
def list_content(did, full_output=False):
    # if full_output is False (default), just return a list of content names in the DID
    # otherwise, return a list of dicts with everythign rucio sends back
    scope, name = did.split(':')
    if full_output:
        content = [d for d in clients.rucio_client.list_content(scope, name)]
    else:
        content = [f"{d['scope']}:{d['name']}" for d in clients.rucio_client.list_content(scope, name)]
    return content


@needs_client
def list_files(did, verbose=False):
    scope, name = did.split(':')
    if verbose:
        files = [f for f in clients.rucio_client.list_files(scope, name)]
    else:
        files = [f['name'] for f in clients.rucio_client.list_files(scope, name)]
    return files


@needs_client
def attach(main_did, attachments, rse=None):
    # attach a list of attachments to the main_did, either a dataset or container
    # the attachments are a list of DIDs
    attachment_dicts = []
    for did in attachments:
        _scope, _name = did.split(':')
        attachment_dicts.append(dict(scope=_scope, name=_name))
    main_scope, main_name = main_did.split(':')
    return clients.rucio_client.attach_dids(main_scope, main_name, attachment_dicts)


@needs_client
def get_size_mb(did):
    # returns size of did (or list of dids) in GB
    if not isinstance(did, str):
        # then assume it's iterable
        total_size = sum([get_size_mb(d) for d in tqdm(did, desc='Getting total size in rucio')])
    else:
        if not isinstance(did, str):
            raise ValueError(f"did must be a string (or an iterable of strings). You passed a {type(did)}")
        scope, name = did.split(':')
        total_size = 0
        for f in clients.rucio_client.list_files(scope, name):
            total_size += int(f['bytes'])/1e6
    return total_size


@needs_client
def list_file_replicas(did, rse=None, **kwargs):
    if 'rse_expression' in kwargs:
        rse_expression = kwargs.pop('rse_expression')
        if kwargs['rse'] != rse:
            raise ValueError(f"You passed rse={rse} and rse_expression={rse_expression}. Pick one.")
    scope, name = did.split(':')
    did_dict = [dict(scope=scope, name=name)]
    replicas = clients.replica_client.list_replicas(did_dict, rse_expression=rse, **kwargs)
    ret = []
    for r in replicas:
        d = dict(name=r['name'], rses=r['rses'])
        ret.append(d)
    return ret


@needs_client
def list_rses():
    return [rse for rse in clients.rse_client.list_rses()]

@needs_client
def get_rse_usage(rse):
    usage = next(clients.rse_client.get_rse_usage(rse))['used']
    return usage

@needs_client
def get_account_usage(account='production', rse=None):
    keep_fields = ['files', 'bytes', 'bytes_limit', 'bytes_remaining']
    if rse is None:
        ret = []
        for rse in list_rses():
            ret.append(get_account_usage(account, rse['rse']))
        return ret
    else:
        usage = clients.account_client.get_local_account_usage(account, rse=rse)
        try:
            usage = next(usage)
            for field in list(usage.keys()):
                if field not in keep_fields:
                    usage.pop(field)
        except StopIteration:
            usage = {field: 0 for field in keep_fields}
        return usage

@needs_client
def get_rse_prefix(rse):
    rse_info = clients.rse_client.get_rse(rse)
    prefix = rse_info['protocols'][0]['prefix']
    return prefix


@needs_client
def get_rse_datasets(rse):
    datasets = clients.replica_client.list_datasets_per_rse(rse)
    ret = []
    for d in tqdm(datasets, desc=f'Finding all datasets at {rse}'):
        ret.append(f"{d['scope']}:{d['name']}")
    return ret


class RucioPermissionError(Exception):
    pass


class RuleNotFoundError(Exception):
    pass


class DataPolicyError(Exception):
    pass

