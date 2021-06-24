import argparse
import logging
import os
import time
import psutil
import pymongo
import pprint
import json

from admix.helper.logger import Logger
import admix.helper.helper as helper
from admix import DEFAULT_CONFIG, __version__
from admix.helper.decorator import NameCollector, ClassCollector
from utilix.config import Config
from admix.interfaces.database import ConnectMongoDB



def version():
    print(__version__)


class UploadManager():

    def __init__(self):

        # Init the runDB
        self.db = ConnectMongoDB()

        self.NORECORDS_DTYPES = helper.get_hostconfig()['norecords_types']
        self.RAW_RECORDS_DTYPES = helper.get_hostconfig()['raw_records_types']
        self.LIGHT_RAW_RECORDS_DTYPES = helper.get_hostconfig()['light_raw_records_types']
        self.RECORDS_DTYPES = helper.get_hostconfig()['records_types']

        #Choose which data type you want to treat
        self.DTYPES = self.NORECORDS_DTYPES + self.RECORDS_DTYPES + self.RAW_RECORDS_DTYPES + self.LIGHT_RAW_RECORDS_DTYPES

        self.threads = []

    def GetDatasetsToUpload(self):

        runs = list(self.db.db.find({'status': { '$in': ['eb_ready_to_upload','transferring']}, 'bootstrax.state': 'done' }, {'number': 1, 'data': 1, 'bootstrax': 1, 'tags': 1}).sort('number',pymongo.ASCENDING))
#        runs = list(self.db.db.find({'status': { '$in': ['eb_ready_to_upload','transferring','transferred']}, 'bootstrax.state': 'done', 'number': {'$gt': 23150} }, {'number': 1, 'data': 1, 'bootstrax': 1, 'tags': 1}).sort('number',pymongo.ASCENDING))

        runs_to_upload = []

        for run in runs:
            run['priority'] = 3
            if 'tags' in run:
                for tag in run['tags']:
                    if 'prioritize' in tag['name']:
                        run['priority'] = 1

        datasets_to_upload = []

        # looping on runs by sorting by priority
        for run in sorted(runs, key=lambda k: k['priority']):

            # Extracts the correct Event Builder machine who processed this run
            bootstrax = run['bootstrax']
            eb = bootstrax['host'].split('.')[0]

            # Look for the first data type available to be uploaded
            for dtype in self.DTYPES:

                # search if dtype still has to be uploaded
                for d in run['data']:
                    if d['type'] == dtype and eb in d['host'] and ('status' not in d or ('status' in d and d['status'] == 'eb_ready_to_upload')):
#                    if d['type'] == dtype and eb in d['host']:
                        dataset_to_upload = {}
                        dataset_to_upload['number'] = run['number']
                        dataset_to_upload['type'] = d['type']
                        file = d['location'].split('/')[-1]
                        hash = file.split('-')[-1]
                        dataset_to_upload['hash'] = hash
                        dataset_to_upload['eb'] = eb
                        dataset_to_upload['priority'] = run['priority']
                        datasets_to_upload.append(dataset_to_upload)

        return(datasets_to_upload)


    def GetThreads(self):

        # Getting the most updated status of processes
        current_threads = []
        for process in psutil.process_iter(['pid', 'name', 'cmdline']):
            if process.info['name']=='admix':
                process_status = {}
                if len(process.info['cmdline'])>2:
                    process_status['task'] = process.info['cmdline'][2]
                    process_status['screen'] = process.parent().parent().parent().parent().cmdline()[-1]
                    current_threads.append(process_status)
        return(current_threads)

    def UpdateThreads(self, threads):
        for thread in threads:
            filename = "/tmp/admix-"+thread['screen']
            if os.path.isfile(filename):
                with open(filename, 'r') as f:
#                    thread['datum'] = json.load(f)
                    thread['datum'] = {}
                    f.close()
        return(threads)

    def AssignDatasetToThread(self, datum, thread_name):
        filename = "/tmp/admix-"+thread_name
        with open(filename, 'w') as f:
            json.dump(datum,f)
            f.close()
    

    def SendAlarm(crashed_threads):
        timestamp = int(time.time())
        print("Alarm on ",time.ctime(timestamp))
        print("List of threads that crashed:\n")
        for thread in crashed_threads:
            print("Task: "+thread['task']+", from screen session: "+thread['screen']+"\n")


    def run(self):

        # Get the list of datasets to upload
        datasets_to_upload = self.GetDatasetsToUpload()
        
        # Show the list of datasets to upload
        print("Runs to upload:")
        for run in datasets_to_upload:
            print(f"Run {run['number']:6d} Type {run['type']:30s} Hash {run['hash']} EB {run['eb']} Priority {run['priority']}")

        # Get the list of threads that are currently running
        current_threads = self.GetThreads()

        # Check if by chance any thread stopped
        crashed_threads = []
        for thread in self.threads:
            thread_found = False
            for current_thread in current_threads:
                if current_thread['screen'] == thread['screen']:
                    thread_found = True
            if not thread_found:
                print("Thread disappeared : ",thread)
                filename = "/tmp/admix-"+thread['screen']
                if os.path.isfile(filename):
                    crashed_threads.append(thread)

        # If there is at least one crashed thread, send the alarm
        if len(crashed_threads)>0:
            self.SendAlarm(crashed_threads)

        # Add to the threads the datasets that they are currently treating
        current_threads = self.UpdateThreads(current_threads)

        # Assign datasets to Upload tasks that are currently available
        for dataset in datasets_to_upload:

            # First, check is the dataset has not been already assigned
            assigned = False
            for thread in current_threads:
                if thread['task']!="Upload":
                    continue
                if 'datum' in thread:
                    if thread['datum']==dataset:
                        assigned = True
            
            # If not, then assign it to the first thread available
            if not assigned:
                for thread in current_threads:
                    if thread['task']!="Upload":
                        continue
                    if 'datum' in thread:
                        if thread['datum']=={}:
                            thread['datum'] = dataset
                            print(f"Assigning run {dataset['number']:6d} Type {dataset['type']:30s} Hash {dataset['hash']} EB {dataset['eb']} to task {thread['screen']}")
                            #self.AssignDatasetToThread(dataset, thread['screen'])
                            break
        
        
        # Print current threads
#        print("Current status:")
#        pp = pprint.PrettyPrinter()
#        pp.pprint(current_threads)

        # Store the current list of threads
        self.threads = current_threads



    def loop(self):

        while True:
            
            self.run()

            if helper.global_dictionary.get('once'):
                break

            # Wait
            wait_time = helper.get_hostconfig()['sleep_time']
            print('Waiting for {0} seconds'.format(wait_time))
            print("You can safely CTRL-C now if you need to stop me")
            try:
                time.sleep(wait_time)
            except KeyboardInterrupt:
                break





def main():
    print("")
    print("--------------------------")
    print("-- aDMIX Upload Manager --")
    print("--------------------------")
    print("")

    parser = argparse.ArgumentParser(description="aDMIX Upload Manager")

    config = Config()

    # From here the input depends on the usage of the command
    # Add arguments for the process manager:
    parser.add_argument('--admix-config', dest="admix_config", type=str, default=config.get('Admix','config_file'),
                        help="Load your host configuration")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run aDMIX Upload Manager only once")
    parser.add_argument('--high', dest='high', action='store_true',
                        help="Treat only high level data types")
    parser.add_argument('--low', dest='low', action='store_true',
                        help="Treat only low level data types")
    args = parser.parse_args()

    #We make the individual arguments global available right after aDMIX starts:
    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    helper.make_global("high", args.high)
    helper.make_global("low", args.low)
    helper.make_global("once", args.once)

    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print(helper.get_hostname())
        print("You are at {0}".format( helper.get_hostname()))
        exit()

    #Setup the logger in a very basic modi
#    lg = Logger(logpath=helper.get_hostconfig()['log_path'],
#                loglevel=logging.DEBUG)
#    lg.Info("-----------------------------------------")
#    lg.Info("aDMIX - advanced Data Management in XENON")
#    helper.make_global("logger", lg)

    upload_manager = UploadManager()
    
    upload_manager.loop()



