# -*- coding: utf-8 -*-
from __future__ import with_statement
import logging
import datetime
import os
import time
import json
import weakref
import numpy as np

from admix.tasks import helper
from admix.tasks.tester import tester
from admix.tasks.dummy import dummy

#from admix.tasks import uploader
#from admix.tasks import rule_updater
#from admix.runDB import xenon_runDB


class Tasker():
    
    def __init__(self):
        self.RegisteredTasks()
        self.GetTaskList()
            
    def ExecuteTasks(self):
        print(self.tasker_list)
        for i_task in self.registeredtasks:
            name = i_task.__class__.__name__
            print("--", name)
            if name in self.tasker_list:
                i_task.run()
            
            
    def ExecuteTask(self, task_name):
        print("execute task")
        print("-", self.registeredtasks)
        print(":", self.tasker_list)
        print("Exec:", task_name)
        #This member function makes the book
        #of the available tasks which are specified in 
        #the configuration file
        
        
        ##Create the query:
        #self.CreateQuery()
        #for i_task in self.registeredtasks:
            #if 

        
        #if self.run_task == 'upload':
            #print("Run an upload session")
            #uploader.Uploader(db_collection=self.GetCollection(),
                              #db_curser=self.GetCursor(),
                              #task_list=self.GetTaskList(),
                              #type_list=self.GetTypeList(),
                              #detk_list=self.GetDetectorList()
                              #).run()
            
        #if self.run_task == 'download':
            #print("Run a download session")
            
        #if self.run_task == 'rule-server':
            #print("Run the rule server")
            
        #if self.run_task == 'rule-updater-1t':
            #logging.info("Run a specific Xenon1T rule updater")
            #rule_updater.RuleUpdater(db_collection=self.GetCollection(),
                                     #db_curser=self.GetCursor(),
                                     #task_list=self.GetTaskList(),
                                     #type_list=self.GetTypeList(),
                                     #dest_list=self.GetDestinationList(),
                                     #detk_list=self.GetDetectorList()
                                     #).run()
    
    def RegisteredTasks(self):
        try:
            self.registeredtasks = [
                                        tester(),
                                        dummy()
                                    ]
        except:
            print("No tasks are registerred to aDMIX!")
            exit()
        return self.registeredtasks
        
    def GetTaskList(self):
        try:
            self.tasker_list = helper.get_hostconfig()['task']
            if isinstance(self.tasker_list, list) == False and self.tasker_list.find(",") > 0:
                tl = self.tasker_list.replace(" ", "").split(",")
                self.tasker_list = []
                for i_task in tl:
                    self.tasker_list.append(i_task)
            elif isinstance(self.tasker_list, list) == False:
                self.tasker_list = [self.tasker_list]
        except:
            print("Specify a task")
            exit()
        return self.tasker_list
    
    def GetTypeList(self):
        try:
            self.type_list = helper.get_hostconfig()['type'].replace(" ", "").split(",")
        except LookupError as e:
            print("No types are specified")
            exit()
            #logging.debug("task_list not specified, running all tasks")
            #return []
        return self.type_list
    
    def GetDetectorList(self):
        try:
            self.detector_list = helper.get_hostconfig()['detector'].replace(" ", "").split(",")
        except LookupError as e:
            print("No detectors are specified")
            exit()
        return self.detector_list
    
    def GetDestinationList(self):
        try:
            self.destination_list = helper.get_hostconfig()['destination'].replace(" ", "").split(",")
        except LookupError as e:
            print("No detectors are specified")
            exit()
        return self.destination_list
                               