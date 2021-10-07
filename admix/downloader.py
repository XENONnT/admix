import os
from argparse import ArgumentParser
from rucio.client.downloadclient import DownloadClient
import socket

from .utils import  xe1t_runs_collection
from .rucio import list_rules, get_did_type
from . import logger
try:
    from straxen import __version__
    straxen_version = __version__
except ImportError:
    print("Straxen not installed in current env, so must pass straxen_version manually")
import time
import utilix

download_client = DownloadClient(logger=logger)

class NoRSEForCountry(Exception):
    pass

class RucioDownloadError(Exception):
    pass


DSET_CACHE = {}

def determine_rse(rse_list):
    # TODO put this in config or something?

    preferred_host_rses = {'rcc': ['UC_DALI_USERDISK', 'UC_OSG_USERDISK', 'SDSC_USERDISK'],
                           'sdsc': ['SDSC_USERDISK', 'UC_OSG_USERDISK', 'UC_DALI_USERDISK'],
                           'in2p3': ['CCIN2P3_USERDISK', 'NIKHEF2_USERDISK', 'CNAF_USERDISK'],
                           'nikhef': ['NIKHEF2_USERDISK', 'SURFSARA_USERDISK', 'CNAF_USERDISK'],
                           'surf': ['SURFSARA_USERDISK', 'NIKHEF2_USERDISK', 'CNAF_USERDISK'],
                          }

    preferred_glidein_rses = {'US,CA':  ['UC_OSG_USERDISK', 'SDSC_USERDISK', 'UC_DALI_USERDISK'],
                              'EUROPE,NL,IT,FR,IL': ['NIKHEF2_USERDISK', 'CNAF_USERDISK', 'SURFSARA_USERDISK']
                              }

    hostname = socket.getfqdn()

    # check if we are running on a specific host that's very close to our rses
    for host, pref_rses in preferred_host_rses.items():
        if host in hostname:
            for rse in pref_rses:
                if rse in rse_list:
                    return rse

    # in case we are on an OSG job, check the GLIDEIN_Country
    glidein_country = os.environ.get('GLIDEIN_Country')
    if glidein_country:
        for country_list, pref_rses in preferred_glidein_rses.items():
            country_list = country_list.split(',')
            if glidein_country in country_list:
                for rse in pref_rses:
                    if rse in rse_list:
                        return rse

    # as last ditch effort, default to UC_OSG or SDSC
    for pref_rse in ['UC_OSG_USERDISK', 'SDSC_USERDISK']:
        if pref_rse in rse_list:
            return pref_rse

    # if get here, return None and let rucio figure it out
    return


def download_dids(dids, num_threads=8, **kwargs):
    # build list of did info
    did_list = []
    for did in dids:
        did_dict = dict(did=did,
                        **kwargs
                        )
        did_list.append(did_dict)
    return download_client.download_dids(did_list, num_threads=num_threads)


def download(did, chunks=None, location='.',  tries=3, metadata=True,
             num_threads=5, rse=None):
    """Function download()

    """
    did_type = get_did_type(did)

    if did_type == 'FILE':
        # make sure we didn't pass certain args
        assert chunks is None, f"You passed the chunks argument, but the DID {did} is FILE"

    if not rse:
        # determine which rses this did is on
        rules = list_rules(did, state='OK')
        rses = [r['rse_expression'] for r in rules]
        # find closest rse
        rse = determine_rse(rses)

    if chunks:
        dids = []
        for c in chunks:
            cdid = f"{did}-{c:06d}"
            dids.append(cdid)
        # also download metadata
        if metadata:
            dids.append(did + '-metadata.json')
    else:
        dids = [did]


    path = did.replace(':', '-')
    # drop the xnt at the beginning
    path = path.replace('xnt_', '')
    if did_type == 'FILE':
        path = '-'.join(path.split('-')[:-1])
    location = os.path.join(location, path)

    os.makedirs(location, exist_ok=True)

    already_exist = []
    for _did in dids:
        if os.path.exists(os.path.join(location, _did.split(':')[1])):
            already_exist.append(_did)

    dids = list(set(dids) - set(already_exist))

    if len(dids) == 0:
        logger.info(f"All files already present at {location}")
        return

    if chunks:
        logger.info(f"Downloading {len(dids)} file{'s'*(len(dids)>1)} of {did} from {rse}")
    else:
        logger.info(f"Downloading {did} from {rse}")

    _try = 1
    success = False
    while _try <= tries and not success:
        if _try == tries:
            rse = None
        try:
            result = download_dids(dids, base_dir=location, no_subdir=True, rse=rse, num_threads=num_threads)
            success = True
        except:
            logger.debug(f"Download try #{_try} failed. Sleeping for {3*_try} seconds.")
            time.sleep(3 ** _try)
            _try += 1

    if success:
        logger.debug(f"Download successful to {location}")
    else:
        raise RucioDownloadError(f"Download of {did} failed")


def get_did_1t(number, dtype):
    query = {'number': number}
    cursor = xe1t_runs_collection.find_one(query, {'number': 1, 'name': 1, 'data': 1})
    for d in cursor['data']:
        if dtype == 'raw':
            if d['type'] == 'raw' and d['host'] == 'rucio-catalogue' and d['status'] == 'transferred':
                return d['location']
        else:
            if (d['type'] == 'processed' and d['host'] == 'rucio-catalogue' and d['pax_version'] == dtype and
                d['status'] == 'transferred'):
                return d['location']

    # if get here, there is a problem finding the DID
    raise ValueError(f"No rucio DID found for run {number} with dtype {dtype}")


def download_1t(number, dtype, location='.',  tries=3, num_threads=5, **kwargs):
    did = get_did_1t(number, dtype)

    # if we didn't pass an rse, determine the best one
    rse = kwargs.pop('rse', None)

    if not rse:
        rules = list_rules(did, state='OK')
        rses = [r['rse_expression'] for r in rules]
        # find closest rse
        rse = determine_rse(rses)

    if dtype == 'raw':
        # get run name
        name = xe1t_runs_collection.find_one({'number': number, 'detector': 'tpc'}, {'name': 1})['name']
        location = os.path.join(location, name)

    os.makedirs(location, exist_ok=True)

    print(f"Downloading {did} from {rse}")

    _try = 1
    success = False

    while _try <= tries and not success:
        if _try == tries:
            rse = None
        result = download_dids(did, base_dir=location, no_subdir=True, rse=rse, num_threads=num_threads)
        if isinstance(result, int):
            print(f"Download try #{_try} failed.")
            time.sleep(5**_try)
            _try += 1

        else:
            success = True

    if success:
        print(f"Download successful to {location}")
    else:
        raise RucioDownloadError("Download of {did} failed")


def main():
    parser = ArgumentParser("admix-download")

    parser.add_argument("number", type=int, help="Run number to download")
    parser.add_argument("dtype", help="Data type to download")
    parser.add_argument("--chunks", nargs="*", help="Space-separated list of chunks to download.")
    parser.add_argument("--dir", help="Path to put the downloaded data.", default='.')
    parser.add_argument('--tries', type=int, help="Number of tries to download the data.", default=3)
    parser.add_argument('--rse', help='RSE to download from')
    parser.add_argument('--threads', help='Number of threads to use', default=3, type=int)
    parser.add_argument('--context', help='strax context you need -- this determines the hash',
                         default='xenonnt_online')
    parser.add_argument('--straxen_version', help='straxen version', default=None)
    parser.add_argument('--experiment', help="xent or xe1t", choices=['xe1t', 'xent'], default='xent')

    args = parser.parse_args()

    if args.experiment == 'xent':
        # use system straxen version if none passed
        version = args.straxen_version if args.straxen_version else straxen_version
        utilix_db = utilix.DB()
        hash = utilix_db.get_hash(args.context, args.dtype, version)
        if args.chunks:
            chunks = [int(c) for c in args.chunks]
        else:
            chunks=None

        download(args.number, args.dtype, hash, chunks=chunks, location=args.dir, tries=args.tries,
                 rse=args.rse, num_threads=args.threads)

    elif args.experiment == 'xe1t':
        download_1t(args.number, args.dtype, location=args.dir, tries=args.tries, rse=args.rse,
                    num_threads=args.threads)
