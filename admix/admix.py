import argparse
import logging
import numpy as np
import os
import time

from admix.helper.logger import Logger
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__

from admix.helper.decorator import NameCollector, ClassCollector
from admix.tasks.example_task import RunExampleTask
from admix.tasks.upload_with_mongodb import UploadMongoDB
from admix.tasks.update_runDB import UpdateRunDBMongoDB
from admix.tasks.init_transfers_with_mongodb import InitTransfersMongoDB
from admix.tasks.download_with_mongodb import DownloadMongoDB
from admix.tasks.clear_transfers_with_mongdb import ClearTransfersMongoDB
from admix.tasks.purge_with_mongodb import PurgeMongoDB
from admix.tasks.upload_from_lngs import UploadFromLNGS
from admix.tasks.fix_upload import FixUpload
from admix.tasks.clean_eb import CleanEB
from admix.tasks.upload_from_lngs_single_thread import UploadFromLNGSSingleThread
from admix.tasks.upload import Upload
from admix.tasks.check_transfers import CheckTransfers
from admix.tasks.move_data_to_rse import MoveDataToRSE
from admix.tasks.monitor_run import MonitorRun
from utilix.config import Config

def version():
    print(__version__)

def your_admix():
    print("advanced Data Management in XENON")

    parser = argparse.ArgumentParser(description="Run your favourite aDMIX")

    config = Config()


    # From here the input depends on the usage of the command
    # Add modules here:
    parser.add_argument('task', nargs="?", default="default",
                        help="Select an aDMIX task")
    # Add arguments for the process manager:
    parser.add_argument('--admix-config', dest="admix_config", type=str, default=config.get('Admix','config_file'),
                        help="Load your host configuration")
    parser.add_argument('--no-update', dest='no_update', action='store_false',
                        help="Add this option to prevent aDMIX updating the Xenon database")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run aDMIX only once")
    parser.add_argument('--high', dest='high', action='store_true',
                        help="Treat only high level data types")
    parser.add_argument('--low', dest='low', action='store_true',
                        help="Treat only low level data types")
    # Add arguments for the individual tasks:
    parser.add_argument('--select-run-numbers', dest='select_run_numbers', type=str,
                        help="Select a range of runs (xxxx1 or xxxx1-xxxx2 or xxxx1-xxxx2,xxxx4-xxxx6)")
    parser.add_argument('--select-run-times', dest='select_run_times', type=str,
                        help="Select a range of runs by timestamps <Date>_<Time>-<Date>_<Time>")
    parser.add_argument('--source', nargs='*', dest='source', type=str,
                        help="Select data according to a certain source(s)")
    parser.add_argument('--type', nargs='*', dest='type', type=str,
                        help="Select data according to a certain type")
    parser.add_argument('--hash', nargs='*', dest='hash', type=str,
                        help="Select data according to a certain hash")
    parser.add_argument('--tag', nargs='*', dest='tag', type=str,
                        help="Select data according to a certain tag(s)")
    parser.add_argument('--destination', dest='destination', type=str,
                        help="Add a destination from ")
    parser.add_argument('--rse', dest='rse', type=str,
                        help="Select your RSE from where to download data")
    parser.add_argument('--lifetime', dest='lifetime', type=str,
                        help="Select your RSE from where to download data")
    parser.add_argument('--force', default=False, action="store_true",
                        help="Enforce your action. Be aware of the application!")
    parser.add_argument('--sleep-time', dest='sleep_time', type=int,
                        help="Time to wait before running again the task")
    args = parser.parse_args()

    #We make the individual arguments global available right after aDMIX starts:
    if args.select_run_numbers != None and args.select_run_times == None:
        helper.make_global("run_numbers", args.select_run_numbers)
    if args.select_run_times != None and args.select_run_numbers == None:
        helper.make_global("run_timestamps", args.select_run_times)

    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("no_db_update", args.no_update)
    helper.make_global("source", args.source)
    helper.make_global("tag", args.tag)
    helper.make_global("type", args.type)
    helper.make_global("hash", args.hash)
    helper.make_global("force", args.force)
    helper.make_global("high", args.high)
    helper.make_global("low", args.low)

    if args.sleep_time != None:
        helper.make_global("sleep_time", args.sleep_time)
    else:
        helper.make_global("sleep_time",helper.get_hostconfig()['sleep_time'])

    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print(helper.get_hostname())
        print("You are at {0}".format( helper.get_hostname()))
        exit()

#    helper.functdef()

    #Setup the logger in a very basic modi
    lg = Logger(logpath=helper.get_hostconfig()['log_path'],
                loglevel=logging.DEBUG)
    lg.Info("-----------------------------------------")
    lg.Info("aDMIX - advanced Data Management in XENON")
    helper.make_global("logger", lg)

    #Determine which tasks are addressed:
    # - if it comes from args.task use it, nevertheless what is defined in hostconfig("task")
    # - if args.task == default use hostconfig("task") information
    task_list = []
    if args.task == "default":
        task_list.extend(helper.get_hostconfig("task"))
    else:
        task_list = [args.task]

    #test if the list of tasks is available from the decorator
    task_test = [True if i_task in NameCollector else False for i_task in task_list]
    task_list = np.array(task_list)[task_test]

    if len(task_list) == 0:
        print("Select a task from this list:")
        for i_task in NameCollector:
            print("  <> {0}".format(i_task))
        print("or adjust the 'task' field in your configuration")
        print("file: {0}".format(helper.global_dictionary["admix_config"]))
        exit()

    #Loop over the inizialization of all classes
    for i_task in task_list:
        ClassCollector[i_task].init()

    #Go for the loop
    while True:

        for i_task in task_list:
            ClassCollector[i_task].run()


        if args.once == True:
            break

        if os.path.exists("/tmp/admix-stop"):
            print("Exiting because of the presence of /tmp/admix-stop file")
            break

        wait_time = helper.global_dictionary['sleep_time']
        if "CheckTransfers" in task_list or "CleanEB" in task_list:
            wait_time = 120

        print('Waiting for {0} seconds'.format(wait_time))
        print("You can safely CTRL-C now if you need to stop me")
        try:
            time.sleep(wait_time)

        except KeyboardInterrupt:
            break


