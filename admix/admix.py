import argparse
import datetime  # remove later if not needed
import logging
import numpy as np
import time
import os

from admix.helper.logger import Logger
import admix
import admix.helper.helper as helper

#from admix.runDB import xenon_runDB as XenonRunDatabase
#from admix.tasks import tester as TestaDMIX

#import the decorators:
from admix.helper.decorator import NameCollector, ClassCollector
#import all your tasks:
from admix.tasks.tester import Tester
from admix.tasks.testDB import TestDB
from admix.tasks.upload_with_mongodb import UploadMongoDB
from admix.tasks.update_runDB import UpdateRunDBMongoDB
from admix.tasks.init_transfers_with_mongodb import InitTransfersMongoDB


def version():
    print("aDMIX is ready for Python3...")
    print("Version:", admix.__version__)

def your_admix():
    print("advanced Data Management in XENON")

    parser = argparse.ArgumentParser(description="Run your favourite aDMIX")

    # From here the input depends on the usage of the command
    # Add modules here:
    parser.add_argument('task', nargs="?", default="default",
                        help="Select an aDMIX task")
    # Add arguments for the process manager:
    parser.add_argument('--admix-config', dest="admix_config", type=str,
                        help="Load your host configuration")
    parser.add_argument('--no-update', dest='no_update', action='store_false',
                        help="Add this option to prevent aDMIX updating the Xenon database")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run the server only once an exits")
    # Add arguments for the individual tasks:
    parser.add_argument('--select-run-numbers', dest='select_run_numbers', type=str,
                        help="Select a range of runs (xxxx1-xxxx2)")
    parser.add_argument('--select-run-times', dest='select_run_times', type=str,
                        help="Select a range of runs by timestamps <Date><Time>-<Date><Time>")
    parser.add_argument('--source', nargs='*', dest='source', type=str,
                        help="Select data according to a certain source(s)")
    parser.add_argument('--tag', nargs='*', dest='tag', type=str,
                        help="Select data according to a certain tag(s)")
    #parser.add_argument('--destination', dest='destination', type=str,
    #                    help="Add a destination from ")
    args = parser.parse_args()

    #We make the individual arguments global available right after aDMIX starts:
    if args.select_run_numbers != None:
        helper.make_global("run_beg", args.select_run_numbers.split("-")[0])
        helper.make_global("run_end", args.select_run_numbers.split("-")[1])
    if args.select_run_times != None:
        helper.make_global("run_start_time", helper.string_to_datatime(args.select_run_times.split("-")[0], '%y%m%d_%H%M') )
        helper.make_global("run_end_time", helper.string_to_datatime(args.select_run_times.split("-")[1], '%y%m%d_%H%M') )

    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("no_db_update", args.no_update)
    helper.make_global("source", args.source)
    helper.make_global("tag", args.tag)


    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print(helper.get_hostname())
        print("You are at {0}".format( helper.get_hostname()))
        exit()

    #Setup the logger in a very basic modi
    lg = Logger(logpath=helper.get_hostconfig()['log_path'],
                loglevel=logging.DEBUG)
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

    #Go for the loop
    while True:

        for i_task in task_list:
            helper.global_dictionary['logger'].Info(f"Run task: {i_task}")
            ClassCollector[i_task].init()
            status = ClassCollector[i_task].run()

            if status == 1:
                pass
                #lg.Error("Module did not start")

            #We might need this later:
            #ClassCollector[i_task].__del__()

        if args.once == True:
            break

