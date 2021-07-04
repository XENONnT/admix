import argparse
import logging
import numpy as np
import os
import time
import psutil

from admix.helper.logger import Logger
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__

from admix.helper.decorator import NameCollector, ClassCollector
from admix.tasks.clean_eb import CleanEB
from admix.tasks.upload import Upload
from admix.tasks.check_transfers import CheckTransfers
from utilix.config import Config

def version():
    print(__version__)

def end_admix():
    process = psutil.Process()
    screen = process.parent().parent().parent().parent().cmdline()[-1]
    os.remove("/tmp/admix-"+screen)
    

def your_admix():
    print("advanced Data Management in XENON")

    parser = argparse.ArgumentParser(description="Run your favourite aDMIX")

    config = Config()

    # From here the input depends on the usage of the command
    parser.add_argument('task', nargs="?", default="default",
                        help="Select an aDMIX task")
    parser.add_argument('--admix-config', dest="admix_config", type=str, default=config.get('Admix','config_file'),
                        help="Load your host configuration")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run aDMIX only once")
    parser.add_argument('--sleep-time', dest='sleep_time', type=int,
                        help="Time to wait before running again the task")
    args = parser.parse_args()

    helper.make_global("admix_config", os.path.abspath(args.admix_config))

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

    #Create a tmp file named as the screen session that contains this process
    process = psutil.Process()
    screen = process.parent().parent().parent().parent().cmdline()[-1]
    open("/tmp/admix-"+screen, 'a').close()

    #Loop over the inizialization of all classes
    for i_task in task_list:
        ClassCollector[i_task].init()

    #Go for the loop
    while True:

        for i_task in task_list:
            ClassCollector[i_task].run()


        if args.once == True:
            end_admix()
            break

        if os.path.exists("/tmp/admix-stop"):
            print("Exiting because of the presence of /tmp/admix-stop file")
            end_admix()
            break

        wait_time = helper.global_dictionary['sleep_time']
        if "CheckTransfers" in task_list or "CleanEB" in task_list:
            wait_time = 600

        print('Waiting for {0} seconds'.format(wait_time))
        print("You can safely CTRL-C now if you need to stop me")
        try:
            time.sleep(wait_time)

        except KeyboardInterrupt:
            end_admix()
            break


