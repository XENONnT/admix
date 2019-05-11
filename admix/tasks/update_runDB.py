# -*- coding: utf-8 -*-
import json
import os
from admix.helper import helper

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

#get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater

import copy

@Collector
class UpdateRunDBMongoDB():

    def __init__(self):
        pass

    def __del__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')

        self.db = ConnectMongoDB()
        self.db.Connect()

        #Init the Rucio data format evaluator in three steps:
        self.rc_reader = ConfigRucioDataFormat()
        self.rc_reader.Config(helper.get_hostconfig('rucio_template'))

        #This class will evaluate your destinations:
        self.destination = Destination()

        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config(helper.get_hostconfig()['template'])

        #Init a class to handle keyword strings:
        self.keyw = Keyword()

        #Init Rucio for later uploads and handling:
        self.rc = RucioSummoner(helper.get_hostconfig("rucio_backend"))
        self.rc.SetRucioAccount(helper.get_hostconfig('rucio_account'))
        #These are important for CLI choice
        #self.rc.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        #self.rc.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        #self.rc.SetHost(helper.get_hostconfig('host'))
        #And get to your Rucio:
        self.rc.ConfigHost()



    def run(self,*args, **kwargs):
        self.init()

        #Decide if you select runs from argsparse or just take everything:
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

        #ToDo We need to get the timestamp selector from helper.global_dictionary into the game here!
        #print( helper.global_dictionary['run_start_time'] )
        #print( helper.global_dictionary['run_end_time'] )
        #Get your collection of run numbers and run names
        collection = self.db.GetRunsByTimestamp(ts_beg, ts_end)

        #Run through the overview collection:
        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']

            dict_name = {}
            dict_name['date'] = r_name.split("_")[0]
            dict_name['time'] = r_name.split("_")[1]

            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]

            print("Test dataset:", r_name, "/", r_number, dict_name, db_info['name'])

            if 'data' not in db_info:
                continue

            #We need to define the right plugins here which are getting updated
            plugin_list = ['raw_records', 'records']
            #adjust the data field with a Rucio update if necessary:
            for i_data in db_info['data']:

                if i_data['host'] != 'rucio-catalogue':
                    continue

                if i_data['type'] not in plugin_list:
                    continue

                # check rules
                if 'location' not in i_data or i_data['location'] == "n/a":
                    print("Skip {0} - Check for location and data consistency".format(i_data['type']))
                    continue

                # Go for the rucio template:
                #ToDo We should do better than deepcopy
                rucio_template = copy.deepcopy(self.rc_reader.GetPlugin(i_data['type']))

                # Fill the key word class with information beforehand:
                #extract the hash
                try:
                    _hash = i_data['location'].split(":")[1].split("-")[1]
                except:
                    _hash = "Xenon1T"

                self.keyw.ResetTemplate()
                self.keyw.SetTemplate(dict_name)
                self.keyw.SetTemplate(db_info)
                self.keyw.SetTemplate({'hash': _hash, 'plugin': i_data['type']})
                self.keyw.SetTemplate({'science_run': helper.get_science_run(db_info['start'])})

                rucio_template = self.keyw.CompleteTemplate(rucio_template)
                  #skip if a type is specified somewhere (config file or command line)
                #if (helper.global_dictionary['plugin_type'] == None) and \
                #    (isinstance(helper.get_hostconfig('type'), list) == True) and \
                #    (len(helper.get_hostconfig('type')) >= 1) and \
                #    (i_data['type'] not in helper.get_hostconfig('type')):
                #    continue

                #if helper.global_dictionary['plugin_type'] != None and i_data['type'] != helper.global_dictionary['plugin_type']:
                #    continue

                rc_rules = self.rc.ListDidRules(rucio_template)

                rule = []
                for i_rule in rc_rules:
                    #Extract the rucio rule information per RSE location
                    #and append

                    rse_expression = i_rule['rse_expression']
                    state = i_rule['state']
                    #print(i_rule)
                    if i_rule['expires_at'] != None:
                        expires_at = i_rule['expires_at'].strftime("%Y-%m-%d-%H:%M:%S")
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
