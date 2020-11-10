import os
import sys
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

    elif glidein_country == "FR":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "NL":
        for site in reversed(EURO_SITES):
            if site in rse_list:
                return site

    elif glidein_country == "IL":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "IT":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    if US_SITES[0] in rse_list:
        return US_SITES[0]
    else:
        raise AttributeError("cannot download data")


def download(number, dtype, hash, chunks=None, location='.',  tries=3, metadata=True,
             num_threads=3, **kwargs):
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
        # find closest one
        rse = determine_rse(rses, os.environ.get('GLIDEIN_Country', 'US'))

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
            _try += 1
            time.sleep(5)
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
    parser.add_argument('--tries', type=int, help="Number of tries to download the data.", default=2)
    parser.add_argument('--rse', help='RSE to download from')
    parser.add_argument('--threads', help='Number of threads to use', default=3, type=int)
    parser.add_argument('--context', help='strax context you need -- this determines the hash',
                         default='xenonnt_online')
    parser.add_argument('--straxen_version', help='straxen version', default=None)

    args = parser.parse_args()

    # use system straxen version if none passed
    version = args.straxen_version if args.straxen_version else straxen_version
    hash = utilix.db.get_hash(args.context, args.dtype, version)
    if args.chunks:
        chunks = [int(c) for c in args.chunks]
    else:
        chunks=None

    download(args.number, args.dtype, hash, chunks=chunks, location=args.dir, tries=args.tries,
             rse=args.rse, num_threads=args.threads)

