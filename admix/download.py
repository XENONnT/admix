import os
from argparse import ArgumentParser
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.database import ConnectMongoDB
from admix.utils.naming import make_did
try:
    from straxen import __version__
    straxen_version = __version__
except ImportError:
    print("Straxen not installed in current env, so must pass straxen_version manually")
import time
import utilix

DB = ConnectMongoDB()

xe1t_coll = utilix.rundb.xe1t_collection()

class NoRSEForCountry(Exception):
    pass


def determine_rse(rse_list, glidein_country):
    # TODO put this in config or something?
    EURO_SITES = ["CCIN2P3_USERDISK",
                  "NIKHEF_USERDISK",
                  "NIKHEF2_USERDISK",
                  "WEIZMANN_USERDISK",
                  "CNAF_USERDISK",
                  "SURFSARA_USERDISK"]

    US_SITES = ["UC_OSG_USERDISK", "UC_DALI_USERDISK"]

    if glidein_country == "US":
        for site in US_SITES:
            if site in rse_list:
                return site
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "CA":
        # Canada
        for site in US_SITES:
            if site in rse_list:
                return site
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "EUROPE":
        for site in EURO_SITES:
            if site in rse_list:
                return site
        for site in US_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "FR":
        for site in EURO_SITES:
            if site in rse_list:
                return site
        for site in US_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "NL":
        for site in reversed(EURO_SITES):
            if site in rse_list:
                return site
        for site in US_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "IL":
        for site in EURO_SITES:
            if site in rse_list:
                return site
        for site in US_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "IT":
        for site in EURO_SITES:
            if site in rse_list:
                return site
        for site in US_SITES:
            if site in rse_list:
                return site

    if US_SITES[0] in rse_list:
        return US_SITES[0]

    return None


def download(number, dtype, hash, chunks=None, location='.',  tries=3, metadata=True,
             num_threads=8, **kwargs):
    """Function download()

    Downloads a given run number using rucio
    :param number: A run number (integer)
    :param dtype: The datatype to download.
    :param chunks: List of integers representing the desired chunks. If None, the whole run will be downloaded.
    :param location: String for the path where you want to put the data. Defaults to current directory.
    :param tries: Integer specifying number of times to try downloading the data. Defaults to 2.
    :param version: Context version as listed in the data_hashes collection
    :param kwargs: Keyword args passed to DownloadDids
    """

    # setup rucio client
    rc = RucioSummoner()

    # get DID
    did = make_did(number, dtype, hash)

    # if we didn't pass an rse, determine the best one
    rse = kwargs.pop('rse', None)
    if not rse:
        # determine which rses this did is on
        rules = rc.ListDidRules(did)
        rses = []
        for r in rules:
            if r['state'] == 'OK':
                rses.append(r['rse_expression'])
        # find closest one, otherwise start at the US end at TAPE
        glidein_region = os.environ.get('GLIDEIN_Country', 'US')
        rse = determine_rse(rses, glidein_region)

    if chunks:
        dids = []
        for c in chunks:
            cdid = did + '-' + str(c).zfill(6)
            dids.append(cdid)
        # also download metadata
        if metadata:
            dids.append(did + '-metadata.json')

    else:
        dids = [did]

    # rename the folder that will be downloaded
    path = did.replace(':', '-')
    # drop the xnt at the beginning
    path = path.replace('xnt_', '')

    location = os.path.join(location, path)
    os.makedirs(location, exist_ok=True)

    # TODO check if files already exist?
    print(did)

    print(f"Downloading {did} from {rse}")

    _try = 1
    success = False

    while _try <= tries and not success:
        if _try == tries:
            rse = None
        result = rc.DownloadDids(dids, download_path=location, no_subdir=True, rse=rse,
                                 num_threads=num_threads, **kwargs)
        if isinstance(result, int):
            print(f"Download try #{_try} failed.")
            time.sleep(5 ** _try)
            _try += 1
        else:
            success = True

    if success:
        print(f"Download successful to {location}")


def get_did_1t(number, dtype):
    query = {'number': number}
    cursor = xe1t_coll.find_one(query, {'number': 1, 'name': 1, 'data': 1})
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


def download_1t(number, dtype, location='.',  tries=3, num_threads=8, **kwargs):
    # setup rucio client
    rc = RucioSummoner()
    did = get_did_1t(number, dtype)

    # if we didn't pass an rse, determine the best one
    rse = kwargs.pop('rse', None)

    if not rse:
        # determine which rses this did is on
        rules = rc.ListDidRules(did)
        rses = []
        for r in rules:
            if r['state'] == 'OK':
                rses.append(r['rse_expression'])
        # find closest one, otherwise start at the US end at TAPE
        glidein_region = os.environ.get('GLIDEIN_Country', 'US')
        rse = determine_rse(rses, glidein_region)

    if dtype == 'raw':
        # get run name
        name = xe1t_coll.find_one({'number': number, 'detector': 'tpc'}, {'name': 1})['name']
        location = os.path.join(location, name)

    os.makedirs(location, exist_ok=True)

    print(f"Downloading {did} from {rse}")

    _try = 1
    success = False

    while _try <= tries and not success:
        if _try == tries:
            rse = None
        result = rc.DownloadDids([did], download_path=location, no_subdir=True, rse=rse,
                                 num_threads=num_threads, **kwargs)
        if isinstance(result, int):
            print(f"Download try #{_try} failed.")
            time.sleep(5**_try)
            _try += 1

        else:
            success = True

    if success:
        print(f"Download successful to {location}")


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
