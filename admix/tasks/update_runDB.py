# -*- coding: utf-8 -*-

import logging
import datetime
import time
#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

import os
import json
from admix.tasks import helper
from admix.interfaces.database import DataBase, ConnectMongoDB
from admix.interfaces.rucioapi import ConfigRucioDataFormat, RucioAPI, RucioCLI, TransferRucio

class update_runDB():

    def __init__(self):
        print('Upload by call starts')

    def init(self):
        self.db = ConnectMongoDB()
        self.db.Connect()

        ##This class will evaluate your destinations:
        #self.destination = Destination()

        ##Since we deal with an experiment, everything is predefine:
        #self.exp_temp = Templater()
        #self.exp_temp.Config( helper.get_hostconfig()['template'] )

        ##Init a class to handle keyword strings:
        #self.keyw = Keyword()

        #Init Rucio for later uploads and handling:
        self.rc = TransferRucio()
        self.rc.SetAccount(helper.get_hostconfig('rucio_account'))
        self.rc.rc_cli.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        self.rc.rc_cli.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        self.rc.rc_cli.SetHost(helper.get_hostconfig('host'))
        self.rc.rc_cli.SetRucioAccount(helper.get_hostconfig('rucio_account'))
        self.rc.rc_cli.ConfigHost()

    def run(self,*args, **kwargs):
        self.init()
        print("-------------------------")
        print("Task: Update runDB")


        #evaluate global_dictionary to create a query to select runs:
        query = {}

        run_numbers = None
        if 'run_numbers' in helper.global_dictionary:
            run_numbers = helper.global_dictionary['run_numbers']
        run_times = None
        if 'run_times' in helper.global_dictionary:
            run_times = helper.global_dictionary['run_times']


        if run_numbers != None and run_times == None:
            #we use run numbers to select here
            rn = helper.run_number_converter(run_numbers)
            rn_beg = None
            rn_end = None
            if len(rn) > 1 and rn[-1]>rn[0]:
                query = { '$and': [ {'number': {'$gte':int(rn[0])}}, {'number': {'$lte':int(rn[-1])}} ] }
            elif (len(rn) > 1 and rn[-1]==rn[0]) or (len(rn)==1):
                query = { 'number': int(rn[0]) }
            elif len(rn) > 1 and rn[-1]<rn[0]:
                print("Your run number selection {0} is wrong".format(helper.global_dictionary['run_numbers']))
                exit()

        elif run_numbers == None and run_times != None:
            #we use the run times to select
            rn = helper.timestamp_converter(run_times)
            try:
                query = { '$and': [ {'start': {'$gte':rn[0][0]}}, {'start': {'$lte':rn[0][1]}} ] }
            except:
                print("Your run times need to be like YYMMDD_HHMM-YYMMDD_HHMM")
                exit()
        elif run_numbers == None and run_times == None:
            query = {}
            
        #Query an overview collection with the relevant information
        #According to the selected timestamps:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'start':True}, from_config=False)

        #collection = self.db.GetQuery(query, sort=[('name',-1)])
        collection = self.db.GetQuery(query)

        #Once you grabbed the overview collection, you want to
        #reset your mongodb projections to include the data field with
        #the locations:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 
                                          'data':True, 'detector':True, 'start':True,
                                          #'data.meta':False
                                          }, from_config=False)
        
        #Run through the overview collection:
        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]

            print("Test dataset:", r_name, "/", r_number)
            
            if 'data' not in db_info:
                continue
            
            #adjust the data field with a Rucio update if necessary:
            for i_data in db_info['data']:
                
                if i_data['host'] != 'rucio-catalogue':
                    continue
                
                #skip if a type is specified somewhere (config file or command line)
                if (helper.global_dictionary['plugin_type'] == None) and \
                    (isinstance(helper.get_hostconfig('type'), list) == True) and \
                    (len(helper.get_hostconfig('type')) >= 1) and \
                    (i_data['type'] not in helper.get_hostconfig('type') ):
                    continue

                if helper.global_dictionary['plugin_type'] != None and i_data['type'] != helper.global_dictionary['plugin_type']:
                    continue
                
                #check rules
                if 'location' not in i_data or i_data['location'] == "n/a":
                    print("Skip {0} - Check for location and data consistency".format(i_data['type']))
                    continue
                
                scope = i_data['location'].split(":")[0]
                dname = i_data['location'].split(":")[1]
                rc_rules = self.rc.rc_api.ListDidRules(scope, dname)
                rule = []
                for i_rule in rc_rules:
                    #Extract the rucio rule information per RSE location
                    #and append
                    rse_expression = i_rule['rse_expression']
                    state = i_rule['state']
                    #print(i_rule)
                    if i_rule['expires_at'] != None:
                        expires_at = datetime.datetime.strptime(i_rule['expires_at'], "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d-%H-%M-%S")
                    else:
                        expires_at = None
                    rule.append("{rse}:{state}:{lifetime}".format(rse=rse_expression,
                                                                      state=state,
                                                                      lifetime=expires_at))
                #Update RSE field if necessary:
                if rule != i_data['rse']:
                    self.db.SetDataField(db_info['_id'], type=i_data['type'], host=i_data['host'], key='rse', value=rule)
                    print("  -> Updated location ({0}) for type {1} to {2}".format(i_data['location'], i_data['type'], rule))
                else:
                    print("  |No update at location ({0}) for type {1} to {2}".format(i_data['location'], i_data['type'], rule))




    def __del__(self):
        print( 'Update runDB stops')
