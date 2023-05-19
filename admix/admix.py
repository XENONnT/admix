import argparse
from admix.daemons.sync import SyncDaemon
from datetime import datetime, timedelta
from admix import __version__


def version(args):
    print(__version__)


def sync(args):
    query = SyncDaemon.query
    if args.run:
        query['number'] = args.run
    if args.days_ago:
        query['start'] = {'$gt': datetime.utcnow() - timedelta(args.days_ago)}

    syncer = SyncDaemon(db_query=query, dtype=args.dtype)
    syncer.single_loop(max_iterations=args.limit, progress_bar=args.progress)


def main():
    parser = argparse.ArgumentParser(description="Main admix command. Use this to call various subcommands.")

    subparsers = parser.add_subparsers(help='sub-command help')

    # version subcommand
    version_parser = subparsers.add_parser('version', help='Print version and exit')
    version_parser.set_defaults(func=version)

    # sync subcommand
    sync_parser = subparsers.add_parser('sync', help='admix sync: to sync rucio with the runsDB')
    sync_parser.add_argument('--run', type=int, help='Run number')
    sync_parser.add_argument('--days_ago', type=int, help='Sync runs taken in the last DAYS_AGO days')
    sync_parser.add_argument('--dtype', help='Only sync the DTYPE strax datatype')
    sync_parser.add_argument('--limit', help='Only sync a max of LIMIT runs', default=0)
    sync_parser.add_argument('--progress', action='store_true', help='Display progress')
    sync_parser.set_defaults(func=sync)

    # now do the thing
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
