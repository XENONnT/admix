# -*- coding: utf-8 -*-

import logging
import datetime
import time

#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

import os
from admix.tasks import helper
from admix.interfaces.database import DataBase, ConnectMongoDB
from admix.interfaces.rucioapi import ConfigRucioDataFormat, RucioAPI, RucioCLI, TransferRucio
from admix.interfaces.templater import Templater
from admix.interfaces.destination import Destination

class upload_by_call():
    
    def __init__(self):
        print('Upload with mongoDB starts')

    def init(self):
        self.db = ConnectMongoDB()
        self.db.Connect()
        
        self.destination = Destination()
        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config( helper.get_hostconfig()['template'] )
        
    def run(self,*args, **kwargs):
        self.init()
        print("-------------------------")
        print("Task: Upload by call")
        
        
        #Decide if you select runs from argsparse or just take everythin:
        if 'run_beg' in helper.global_dictionary:
            run_beg = helper.global_dictionary['run_beg']
        else:
            run_beg = self.db.GetSmallest('number')
        
        if 'run_end' in helper.global_dictionary:
            run_end = helper.global_dictionary['run_end']
        else:
            run_end = self.db.GetLargest('number')
        
        #When you found your run numbers, you want to select timestamps from them
        #This allows to catch everything in between
        ts_beg = self.db.FindTimeStamp('number', int(run_beg) )
        ts_end = self.db.FindTimeStamp('number', int(run_end) )
    
        #Query an overview collection with the relevant information
        #According to the selected timestamps:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'start':True}, from_config=False)
        query = { '$and': [ {'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}} ] }
        #collection = self.db.GetQuery(query, sort=[('name',-1)])
        collection = self.db.GetQuery(query)

        #Once you grabbed the overview collection, you want to
        #reset your mongodb projections to include the data field with
        #the locations:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'data':True, 'detector':True, 'start':True}, from_config=False)
        
        #Run through the overview collection:
        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]
            
            for i_data in db_info['data']:
                
                
                dest = self.destination.EvalDestination(host=helper.get_hostconfig('host'),
                                                        origin=i_data['host'],
                                                        destination=helper.global_dictionary['destination'])
            
                if dest == None:
                    continue
                
                print(r_name, r_number)
                print("----")
                print("  -", i_data['host'], i_data['location'])
                print(dest)
                
                #pre infos:
                origin_type     = i_data['type'] #This is the plugin name which is gonna be handled (uploaded)
                origin_location = i_data['location'] #This holds the physical location of the plugin folder
                origin_host     = i_data['host']
                origin_status   = i_data['status']
                
                print(origin_type, origin_location, origin_host, origin_status)
                
                
                #Extract the template information according the pre-defined physical file structure:
                template_info = self.exp_temp.GetTemplateEval(plugin=origin_type,
                                                              host=origin_host,
                                                              string=origin_location)
                print(template_info)
                
                #1) Get experimental dependend rucio structure which needs to be fulfilled
                rucio_exp_config_path = helper.get_hostconfig('rucio_template')
                    
                rc_reader = ConfigRucioDataFormat()
                rc_reader.Config(rucio_exp_config_path)
                print("rucio types:", rc_reader.GetTypes())
                rucio_form = rc_reader.GetStructure()[origin_type]
                print(rucio_form)
                #Setup the runDB entry via another class:
                
                #Set to transferring:
                
                #Start upload
                
                #Apply further rules
                
                #Update runDB with Rucio
                
                #Set to transferred
            
            
    def __del__(self):
        print( 'Upload by call stops')