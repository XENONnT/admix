import os
from argparse import ArgumentParser
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.database import ConnectMongoDB
from admix.utils.naming import make_did
from admix.utils.list_file_replicas import list_file_replicas


def showrun(number, dtype, hash, rse):
    """Function showrun()
    
    Show the full path of a given run number using rucio
    :param number: A run number (integer)
    :param dtype: The datatype to show
    :param hash: The hash to show
    """

    # setup rucio client
#    rc = RucioSummoner()
    db = ConnectMongoDB()
    
    files = list_file_replicas(number, dtype, hash)
#    files = list_file_replicas(number, dtype, hash,'UC_DALI_USERDISK')

    print(files)
    

def main():
    parser = ArgumentParser("showrun")

    parser.add_argument("number", type=int, help="Run number to show")
    parser.add_argument("dtype", help="Data type to show")
    parser.add_argument("hash", help="Hash to show")
    parser.add_argument('--rse', help='RSE to look from')

    args = parser.parse_args()

    helper.make_global("admix_config", os.path.abspath(DEFAULT_CONFIG))
    #print(helper.get_hostconfig()['database'])

    showrun(args.number, args.dtype, args.hash, rse=args.rse)

