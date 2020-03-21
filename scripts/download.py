import os
import time
import shutil
from argparse import ArgumentParser

from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.keyword import Keyword
from admix.interfaces.database import MongoDB
from admix.utils import make_did

DB = MongoDB()


def download(number, dtype, chunks=None, location='.',  tries=2, **kwargs):
    """Function download()
    
    Downloads a given run number using rucio
    :param number: A run number (integer)
    :param dtype: The datatype to download.
    :param chunks: List of integers representing the desired chunks. If None, the whole run will be downloaded.
    :param location: String for the path where you want to put the data. Defaults to current directory.
    :param tries: Integer specifying number of times to try downloading the data. Defaults to 2.
    :param kwargs: Keyword args passed to DownloadDids
    """

    # setup rucio client
    rc = RucioSummoner()

    # Get the DID from the runsDB using the number and data type
    # Warning: be mindful of different versions. This assumes we maintain the runDB 'dids' field correctly.
    did = DB.GetDid(number, dtype)

    # TODO determine which rse to download from?

    if chunks:
        dids = []
        for c in chunks:
            cdid = did + '-' + str(c).zfill(6)
            dids.append(cdid)

    else:
        dids = [did]

    # rename the folder that will be downloaded
    path = did.replace(':', '_')

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

    print(f"Download successful to {location}")


def main():
    parser = ArgumentParser("admix-download")

    parser.add_argument("number", type=int, help="Run number to download")
    parser.add_argument("dtype", help="Data type to download")
    parser.add_argument("--chunks", nargs="*", help="Space-separated list of chunks to download.")
    parser.add_argument("--location", help="Path to put the downloaded data.", default='.')
    parser.add_argument('--tries', type=int, help="Number of tries to download the data.", default=2)

    args = parser.parse_args()

    if args.chunks:
        chunks = [int(c) for c in args.chunks]
    else:
        chunks=None


    download(args.number, args.dtype, chunks=chunks, location=args.location, tries=args.tries)


if __name__ == "__main__":

    number = 7158
    dtype = 'raw_records_mv'
    location = '/home/datamanager'
    download(number, dtype, location=location)
