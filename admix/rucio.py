"""
Contains rucio commands with XENON-specific wrappers
"""
from datetime import datetime
from functools import wraps
from tqdm import tqdm
from rucio.client.client import Client
from rucio.client.replicaclient import ReplicaClient
from rucio.client.accountclient import AccountClient
from rucio.client.rseclient import RSEClient
from . import logger
from .utils import from_did, db


rucio_client = Client()
replica_client = ReplicaClient()
account_client = AccountClient()
rse_client = RSEClient()


def requires_production(func):
    """Decorator used for functions that require the production account credentials"""
    def wrapped(*args, **kwargs):
        if rucio_client.account != 'production':
            raise RucioPermissionError(f"You must be the production user to call {func.__name__}")
        return func(*args, **kwargs)
    return wrapped


def build_data_dict(did, rse, status):
    number, dtype, h = from_did(did)
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
            number, dtype, h = from_did(did)
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


def list_rules(did, **filters):
    scope, name = did.split(':')
    rules = rucio_client.list_did_rules(scope, name)
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


def get_rses(did, **filters):
    rules = list_rules(did, **filters)
    rses = [r['rse_expression'] for r in rules]
    return rses


def get_rule(did, rse):
    rules = list_rules(did, rse_expression=rse)
    if len(rules) == 0:
        logger.debug(f"No rule found for {did} at {rse}!")
        return
    return rules[0]


@requires_production
@update_db('add')
def add_rule(did, rse, copies=1, update_db=False, quiet=False, **kwargs):
    scope, name = did.split(':')
    did_dict = dict(scope=scope, name=name)
    rucio_client.add_replication_rule([did_dict], copies, rse_expression=rse, **kwargs)
    if not quiet:
        print(f"Replication rule added for {did} at {rse}")


@requires_production
@update_db('delete')
def delete_rule(did, rse, purge_replicas=True, _careful=True, _required_copies=1, update_db=False):
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
    rucio_client.delete_replication_rule(rule['id'], purge_replicas=purge_replicas)
    print(f"Replication rule for {did} at {rse} removed.")


@requires_production
@update_db('add')
def add_conditional_rule(did, rse, from_rse=None, update_db=False, **kwargs):
    """Convenience wrapper around add_rule"""
    add_rule(did, rse, source_replica_expression=from_rse, **kwargs)


@requires_production
@update_db('move')
def move_rule(did, rse, from_rse, update_db=False):
    pass


@requires_production
def add_scope(scope):
    return rucio_client.add_scope('production', scope)


@requires_production
def add_container(scope, name, **kwargs):
    return rucio_client.add_container(scope, name, **kwargs)


def list_datasets(scope):
    datasets = [d for d in rucio_client.list_dids(scope, {'type': 'dataset'}, type='dataset')]
    return datasets


def list_containers(scope):
    containers = [d for d in rucio_client.list_dids(scope, {'type': 'container'}, type='container')]
    return containers


def list_content(did, full_output=False):
    # if full_output is False (default), just return a list of content names in the DID
    # otherwise, return a list of dicts with everythign rucio sends back
    scope, name = did.split(':')
    if full_output:
        content = [d for d in rucio_client.list_content(scope, name)]
    else:
        content = [f"{d['scope']}:{d['name']}" for d in rucio_client.list_content(scope, name)]
    return content


def list_files(did, verbose=False):
    scope, name = did.split(':')
    if verbose:
        files = [f for f in rucio_client.list_files(scope, name)]
    else:
        files = [f['name'] for f in rucio_client.list_files(scope, name)]
    return files


def attach(main_did, attachments, rse=None):
    # attach a list of attachments to the main_did, either a dataset or container
    # the attachments are a list of DIDs
    attachment_dicts = []
    for did in attachments:
        _scope, _name = did.split(':')
        attachment_dicts.append(dict(scope=_scope, name=_name))
    main_scope, main_name = main_did.split(':')
    return rucio_client.attach_dids(main_scope, main_name, attachment_dicts)


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
        for f in rucio_client.list_files(scope, name):
            total_size += int(f['bytes'])/1e6
    return total_size


def list_file_replicas(did, rse=None, **kwargs):
    if 'rse_expression' in kwargs:
        rse_expression = kwargs.pop('rse_expression')
        if kwargs['rse'] != rse:
            raise ValueError(f"You passed rse={rse} and rse_expression={rse_expression}. Pick one.")
    scope, name = did.split(':')
    did_dict = [dict(scope=scope, name=name)]
    replicas = replica_client.list_replicas(did_dict, rse_expression=rse, **kwargs)
    ret = []
    for r in replicas:
        d = dict(name=r['name'], rses=r['rses'])
        ret.append(d)
    return ret


def get_account_usage(account='production', rse=None):
    """We need to update rucio server first"""
    raise NotImplementedError
    #account_client.get_global_account_usage(account, rse_expression=rse)


def get_account_limits(account='production'):
    """We need to update rucio server first"""
    raise NotImplementedError
    #account_client.get_global_account_limit(account)


def get_rse_prefix(rse):
    rse_info = rse_client.get_rse(rse)
    prefix = rse_info['protocols'][0]['prefix']
    return prefix


def get_rse_datasets(rse):
    datasets = replica_client.list_datasets_per_rse(rse)
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


