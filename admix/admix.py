# -*- coding: utf-8 -*-
from __future__ import absolute_import              #Necessary for import of modules (see module section)

import logging
import argparse
import time
import os
"""Main module section"""
from admix.runDB import xenon_runDB as XenonRunDatabase
from admix.tasks import tester as TestaDMIX
#from admix.tasks import uploader as uploader
from admix.tasks import helper
from admix.tasks import tasker

##import admix
#import admix.tasks as tester

def version():
    print("aDMIX")
    
def tester():
    tt = TestaDMIX.TestaDMIX()
    tt.PrintTester()
    
def uploader():
    pass

def downloader():
    pass

def server():
    
    parser = argparse.ArgumentParser(description="Run your favourite aDMIX task in a loop!")
    
    #From here the input depends on the usage of the command
    parser.add_argument('--admix-config', dest="admix_config", type=str,
                        help="Load your host configuration")
    parser.add_argument('--once', dest='once', action='store_true',
                        help="Run the server only once an exits")

    parser.add_argument('--run', dest='run', type=str,
                        help="Xenon1T run number")
    parser.add_argument('--name', dest='name', type=str,
                        help="Xenon1T run name")
    parser.add_argument('--timestamp', dest='timestamp', type=str,
                        help="Select a range of times in where the runs are in")
    
    args = parser.parse_args()
    helper.make_global("admix_config", os.path.abspath(args.admix_config))
    _run          = helper.run_number_converter(args.run)
    _name         = helper.run_name_converter(args.name)
    _timestamp    = helper.run_timestampe_converter(args.timestamp)
    _once         = args.once
    
    
    #Pre tests:
    # admix host configuration must match the hostname:
    if helper.get_hostconfig()['hostname'] != helper.get_hostname():
        print("admix configuration file for %s" % helper.get_hostconfig()['hostname'])
        print("You are at % " % helper.get_hostname() )
        exit()
    
    # Ping rucio
    
    # Ping Xenon Database
    
    #Finish pre tests
    
    #Load the database
    #xrd = XenonRunDatabase.XenonRunDatabase()
    
    while True: # yeah yeah
        print("Load the database")
        #xrd.LoadCollection()
    
    
        #xrd.QueryByTimestamp(_timestamp)
        #xrd.QueryByRunnumber(_run)
        #xrd.QueryByRunname(_name)
        #xrd.CreateQuery()
        
        #cursor = xrd.GetCursor()
        #print("load:", len(cursor))
        
        #Fill in download routines:
        task_m = tasker.Tasker()
        
        task_m.LoadCollection()    
        task_m.QueryByTimestamp(_timestamp)
        task_m.QueryByRunnumber(_run)
        task_m.QueryByRunname(_name)
        
        #tasker.SetDataSelection(cursor)
        task_m.ExecuteTask()
        #tasker.GetTypeList()
        #if helper.executer():
            #continue
        
        
        if _once:
            break
        else:
            print('Sleeping. (5s)')
        time.sleep(5)