import logging
import argparse
import os


"""Main module section"""
import admix

from admix.interfaces.rucioapi import ConfigRucioDataFormat, RucioAPI, RucioCLI, TransferRucio
from admix.tasks.tasker import Tasker
#from admix.runDB import xenon_runDB as XenonRunDatabase
#from admix.tasks import tester as TestaDMIX
from admix.tasks import helper


import time

def version():
    print("aDMIX is ready for Python3...")
    print("Version:", admix.__version__)

#def tester():
    #print("I am your aDMIX tester")
    #rc_reader = ConfigRucioDataFormat()

    #rc_reader.Config("admix/config/xenon1t_format.config")

    #print(rc_reader.GetTypes())
    #print(rc_reader.GetStructure())
    ##tt = TestaDMIX.TestaDMIX()
    ##tt.PrintTester()

    ##t = RucioConfig()
    ##t.SetRucioConfig("admix/config/rucio_cli.config")

    ##rc = RucioAPI()
    #rcli = RucioCLI()
    #rcli.SetConfigPath("/home/bauermeister/Development/admix/admix/config/rucio_cli/")
    #rcli.SetHost("midway2")
    #rcli.SetProxyTicket("/home/bauermeister/proxy_xenon/x509up_own")
    #rcli.SetRucioAccount("production")
    #rcli.ConfigHost()


def tester():
    print("Develop aDMIX service package")

    parser = argparse.ArgumentParser(description="Run your favourite aDMIX task in a loop!")

    #From here the input depends on the usage of the command
    parser.add_argument('--admix-config', dest="admix_config", type=str,
                        help="Load your host configuration")
    parser.add_argument('--no-update', dest='no_update', action='store_false',
                        help="Add this option to prevent aDMIX updating the Xenon database")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run the server only once an exits")
    parser.add_argument('--select-run-numbers', dest='select_run_numbers', type=str,
                        help="Select a range of runs (xxxx1-xxxx2)")
    parser.add_argument('--select-run-times', dest='select_run_times', type=str,
                        help="Select a range of runs by timestamps")


    args = parser.parse_args()

    if args.select_run_numbers != None:
        helper.make_global("run_beg", args.select_run_numbers.split("-")[0])
        helper.make_global("run_end", args.select_run_numbers.split("-")[1])
    if args.select_run_times != None:
        helper.make_global("run_start_time", args.select_run_times.split("-")[0])
        helper.make_global("run_end_time", args.select_run_times.split("-")[1])

    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("no_db_update", args.no_update)
    _once = args.once


    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print("You are at % " % helper.get_hostname() )
        exit()

    ##Setup the log file (like in cax):
    admix_version = 'admix_v%s - ' % admix.__version__
    logging.basicConfig(filename=helper.get_hostconfig()['log_path'],
                        level=logging.INFO,
                        format=admix_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('aDMIX is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logging.getLogger("requests").setLevel(logging.ERROR)

    #Check for a sleep time:
    if 'sleep_time' in helper.get_hostconfig():
        sleep_time = helper.get_hostconfig()['sleep_time']
        logging.info("Sleep time from the admix configuration file is %s seconds" % str(sleep_time) )
    else:
        logging.info("Sleep time uses pre defined value of %s seconds" % str(sleep_time) )
        sleep_time = 5

    # Ping rucio
      #todo ?
    # Ping Xenon Database
      #todo ?
    #Finish pre tests

    #runDB


    while True: # yeah yeah
        print("cycle")
        task_m = Tasker()
        task_m.ExecuteTasks()

        if _once:
            break
        else:
            logging.info('Sleeping. ({t} seconds)'.format(t=sleep_time) )
        time.sleep(sleep_time)

def manuell_transfer():
    print("Set up a transfer manually in the database")

    parser = argparse.ArgumentParser(description="Input commands to setup a transfer manually.")

    #From here the input depends on the usage of the command
    parser.add_argument('--admix-config', dest="admix_config", type=str,
                        help="Load your host configuration")
    parser.add_argument('--no-update', dest='no_update', action='store_false',
                        help="Add this option to prevent aDMIX updating the Xenon database")
    parser.add_argument('--select-run', dest='select_run', type=str,
                        help="Select a single run")
    parser.add_argument('--select-run-numbers', dest='select_run_numbers', type=str,
                        help="Select a range of runs (xxxx1-xxxx2)")
    parser.add_argument('--select-run-times', dest='select_run_times', type=str,
                        help="Select a range of runs by timestamps")


    args = parser.parse_args()

    if args.select_run_numbers != None and args.select_run == None:
        helper.make_global("run_beg", args.select_run_numbers.split("-")[0])
        helper.make_global("run_end", args.select_run_numbers.split("-")[1])
    if args.select_run_numbers == None and args.select_run != None:
        helper.make_global("run_beg", args.select_run)
        helper.make_global("run_end", args.select_run)
    if args.select_run_times != None:
        helper.make_global("run_start_time", args.select_run_times.split("-")[0])
        helper.make_global("run_end_time", args.select_run_times.split("-")[1])

    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("no_db_update", args.no_update)



    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print("You are at % " % helper.get_hostname() )
        exit()

    ##Setup the log file (like in cax):
    admix_version = 'admix_v%s - ' % admix.__version__
    logging.basicConfig(filename=helper.get_hostconfig()['log_path'],
                        level=logging.INFO,
                        format=admix_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('aDMIX is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logging.getLogger("requests").setLevel(logging.ERROR)


    task_m = Tasker()
    task_m.ExecuteTask("set_manuell_transfers")





def upload_by_call():
    print("Develop aDMIX service package")

    parser = argparse.ArgumentParser(description="Run your favourite aDMIX task in a loop!")

    #From here the input depends on the usage of the command
    parser.add_argument('--admix-config', dest="admix_config", type=str,
                        help="Load your host configuration")
    parser.add_argument('--no-update', dest='no_update', action='store_false',
                        help="Add this option to prevent aDMIX updating the Xenon database")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run the server only once an exits")
    parser.add_argument('--select-run-numbers', dest='select_run_numbers', type=str,
                        help="Select a range of runs (xxxx1-xxxx2)")
    parser.add_argument('--select-run-times', dest='select_run_times', type=str,
                        help="Select a range of runs by timestamps")
    parser.add_argument('--destination', dest='destination', type=str,
                        help="Put in your destination manually")
    parser.add_argument('--plugin-type', dest='plugin_type', type=str,
                        help="Put in your plugin type of choice manually")



    args = parser.parse_args()

    helper.make_global("run_numbers", args.select_run_numbers)
    helper.make_global("run_times", args.select_run_times)
    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("no_db_update", args.no_update)
    helper.make_global("destination", args.destination)
    helper.make_global("plugin_type", args.plugin_type)
    _once = args.once


    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print("You are at % " % helper.get_hostname() )
        exit()

    ##Setup the log file (like in cax):
    admix_version = 'admix_v%s - ' % admix.__version__
    logging.basicConfig(filename=helper.get_hostconfig()['log_path'],
                        level=logging.INFO,
                        format=admix_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('aDMIX is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logging.getLogger("requests").setLevel(logging.ERROR)

    #Check for a sleep time:
    if 'sleep_time' in helper.get_hostconfig():
        sleep_time = helper.get_hostconfig()['sleep_time']
        logging.info("Sleep time from the admix configuration file is %s seconds" % str(sleep_time) )
    else:
        logging.info("Sleep time uses pre defined value of %s seconds" % str(sleep_time) )
        sleep_time = 5

    while True: # yeah yeah
        task_m = Tasker()
        task_m.ExecuteTask("upload_by_call")

        if _once:
            break
        else:
            logging.info('Sleeping. ({t} seconds)'.format(t=sleep_time) )
        time.sleep(sleep_time)


def database_entries():
    print("Develop aDMIX service package")

    parser = argparse.ArgumentParser(description="Run your favourite aDMIX task in a loop!")

    #From here the input depends on the usage of the command
    parser.add_argument('--admix-config', dest="admix_config", type=str,
                        help="Load your host configuration")
    parser.add_argument('--no-update', dest='no_update', action='store_false',
                        help="Add this option to prevent aDMIX updating the Xenon database")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run the server only once an exits")
    parser.add_argument('--select-run-numbers', dest='select_run_numbers', type=str,
                        help="Select a range of runs (xxxx1-xxxx2)")
    parser.add_argument('--select-run-times', dest='select_run_times', type=str,
                        help="Select a range of runs by timestamps")



    args = parser.parse_args()

    if args.select_run_numbers != None:
        helper.make_global("run_beg", args.select_run_numbers.split("-")[0])
        helper.make_global("run_end", args.select_run_numbers.split("-")[1])
    if args.select_run_times != None:
        helper.make_global("run_start_time", args.select_run_times.split("-")[0])
        helper.make_global("run_end_time", args.select_run_times.split("-")[1])

    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("no_db_update", args.no_update)
    _once = args.once


    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print("You are at % " % helper.get_hostname() )
        exit()

    ##Setup the log file (like in cax):
    admix_version = 'admix_v%s - ' % admix.__version__
    logging.basicConfig(filename=helper.get_hostconfig()['log_path'],
                        level=logging.INFO,
                        format=admix_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('aDMIX is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logging.getLogger("requests").setLevel(logging.ERROR)

    task_m = Tasker()
    task_m.ExecuteTask("database_entries")


#def uploader():
    #pass

#def downloader():
    #pass

#def server():

    #parser = argparse.ArgumentParser(description="Run your favourite aDMIX task in a loop!")

    ##From here the input depends on the usage of the command
    #parser.add_argument('--admix-config', dest="admix_config", type=str,
                        #help="Load your host configuration")
    #parser.add_argument('--once', dest='once', action='store_true',
                        #help="Run the server only once an exits")
    #parser.add_argument('--no-update', dest='no_update', action='store_false',
                        #help="Add this option to prevent aDMIX updating the Xenon database")

    #parser.add_argument('--run', dest='run', type=str,
                        #help="Xenon1T run number")
    #parser.add_argument('--name', dest='name', type=str,
                        #help="Xenon1T run name")
    #parser.add_argument('--timestamp', dest='timestamp', type=str,
                        #help="Select a range of times in where the runs are in")

    #args = parser.parse_args()
    #helper.make_global("admix_config", os.path.abspath(args.admix_config))
    #helper.make_global("no_db_update", args.no_update)
    #_run          = helper.run_number_converter(args.run)
    #_name         = helper.run_name_converter(args.name)
    #_timestamp    = helper.run_timestampe_converter(args.timestamp)
    #_once         = args.once


    ##Pre tests:
    ## admix host configuration must match the hostname:
    #if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        #print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        #print("You are at % " % helper.get_hostname() )
        #exit()

    ###Setup the log file (like in cax):
    #admix_version = 'admix_v%s - ' % __version__
    #logging.basicConfig(filename=helper.get_hostconfig()['log_path'],
                        #level=logging.INFO,
                        #format=admix_version + '%(asctime)s [%(levelname)s] '
                                             #'%(message)s')
    #logging.info('aDMIX is starting')

    ## define a Handler which writes INFO messages or higher to the sys.stderr
    #console = logging.StreamHandler()
    #console.setLevel(logging.INFO)
    #formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    ## tell the handler to use this format
    #console.setFormatter(formatter)
    ## add the handler to the root logger
    #logging.getLogger('').addHandler(console)
    #logging.getLogger("requests").setLevel(logging.ERROR)

    ##Check for a sleep time:
    #if 'sleep_time' in helper.get_hostconfig():
        #sleep_time = helper.get_hostconfig()['sleep_time']
        #logging.info("Sleep time from the admix configuration file is %s seconds" % str(sleep_time) )
    #else:
        #logging.info("Sleep time uses pre defined value of %s seconds" % str(sleep_time) )
        #sleep_time = 5

    ## Ping rucio
      ##todo ?
    ## Ping Xenon Database
      ##todo ?
    ##Finish pre tests



    #while True: # yeah yeah

        #task_m = tasker.Tasker()

        #task_m.LoadCollection()
        #task_m.QueryByTimestamp(_timestamp)
        #task_m.QueryByRunnumber(_run)
        #task_m.QueryByRunname(_name)
        #task_m.ExecuteTask()

        #if _once:
            #break
        #else:
            #logging.info('Sleeping. ({t} seconds)'.format(t=sleep_time) )
        #time.sleep(sleep_time)
