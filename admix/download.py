import os
from argparse import ArgumentParser
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.database import ConnectMongoDB
from admix.utils.naming import make_did

DB = ConnectMongoDB()


def download(number, dtype, hash=None, chunks=None, location='.',  tries=3,  version='latest',
             **kwargs):
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


    # get the DID
    # this assumes we always keep the same naming scheme
    # if no hash is passed, get it from the database
    if not hash:
        hash = DB.GetHash(dtype, version=version)

    did = make_did(number, dtype, hash)

    # TODO determine which rse to download from?

    if chunks:
        dids = []
        for c in chunks:
            cdid = did + '-' + str(c).zfill(6)
            dids.append(cdid)

    else:
        dids = [did]

    # rename the folder that will be downloaded
    path = did.replace(':', '-')
    # drop the xnt at the beginning
    path = path.replace('xnt_', '')

    location = os.path.join(location, path)
    os.makedirs(location, exist_ok=True)

    print(f"Downloading {did}")

    _try = 1
    success = False

    while _try <= tries and not success:
        result = rc.DownloadDids(dids, download_path=location, no_subdir=True, **kwargs)
        if isinstance(result, int):
            print(f"Download try #{_try} failed.")
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
    parser.add_argument("--location", help="Path to put the downloaded data.", default='.')
    parser.add_argument('--tries', type=int, help="Number of tries to download the data.", default=2)
    parser.add_argument('--rse', help='RSE to download from')

    args = parser.parse_args()

    if args.chunks:
        chunks = [int(c) for c in args.chunks]
    else:
        chunks=None

    download(args.number, args.dtype, chunks=chunks, location=args.location, tries=args.tries,
             rse=args.rse)

