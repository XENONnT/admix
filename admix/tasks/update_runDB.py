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

        # We want the first and the last run:
        self.gboundary = self.db.GetBoundary()
        self.run_nb_min = self.gboundary['min_number']
        self.run_nb_max = self.gboundary['max_number']
        self.run_ts_min = self.gboundary['min_start_time']
        self.run_ts_max = self.gboundary['max_start_time']

        #Init the Rucio data format evaluator in three steps:
        self.rc_reader = ConfigRucioDataFormat()
        self.rc_reader.Config(helper.get_hostconfig('rucio_template'))
        self.defined_types = self.rc_reader.GetTypes()

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
        self.rc.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        self.rc.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        self.rc.SetHost(helper.get_hostconfig('host'))
        self.rc.ConfigHost()



    def run(self,*args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')

        ts_beg = None
        ts_end = None
        if helper.global_dictionary.get('run_numbers') != None:
            #Evaluate terminal input for run number assumption (terminal input == true)
            true_nb_beg, true_nb_end = helper.eval_run_numbers(helper.global_dictionary.get('run_numbers'),
                                                               self.run_nb_min,
                                                               self.run_nb_max)
            #Get the timestamps from the run numbers:
            ts_beg = self.db.FindTimeStamp('number', int(true_nb_beg))
            ts_end = self.db.FindTimeStamp('number', int(true_nb_end))

        elif helper.global_dictionary.get('run_timestamps') != None:
            #Evaluate terminal input for run name assumption
            true_ts_beg, true_ts_end = helper.eval_run_timestamps(helper.global_dictionary.get('run_timestamps'),
                                                               self.run_ts_min,
                                                               self.run_ts_max)
            ts_beg = true_ts_beg
            ts_end = true_ts_end

        elif helper.global_dictionary.get('run_timestamps') == None and \
            helper.global_dictionary.get('run_numbers') == None:
            ts_beg = self.run_ts_min
            ts_end = self.run_ts_max
        else:
            helper.global_dictionary['logger'].Error("Check for your input arguments (--select-run-number or --select-run-time")
            exit(1)
            #exection

        #Get your collection of run numbers and run names
        collection = self.db.GetRunsByTimestamp(ts_beg, ts_end)



        # We need to define the right plugins here which are getting updated:
        # Therefore we have two options!
        # 1) Use all types which are defined in Rucio format configuration file
        # 2) Use the command line argument inputs
        if helper.global_dictionary.get('type') != None and isinstance(helper.global_dictionary.get('type'), list):
            plugin_list = helper.global_dictionary.get('type')
        else:
            plugin_list = self.defined_types

        #plugin_list = ['raw_records', 'records']

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

            if 'data' not in db_info:
                continue

            helper.global_dictionary['logger'].Info(f"Update data set {r_name}/{r_number}")
            #adjust the data field with a Rucio update if necessary:
            for i_data in db_info['data']:

                if i_data['host'] != 'rucio-catalogue':
                    continue

                if i_data['type'] not in plugin_list:
                    continue

                # check rules
                if 'location' not in i_data or i_data['location'] == "n/a":
                    helper.global_dictionary['logger'].Info("Skip {0} - Check for location and data consistency".format(i_data['type']))
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

                #This section is adjust for XENON1T legacy support:
                # The detector name muon_veto from the database changes into mv
                # Fix one minute bug from past XENON1T days...
                # Fix the science run definition hole between SR0 and SR1
                sr_run_selected = helper.get_science_run(db_info['start'])
                if _hash == 'Xenon1T':
                    db_info = helper.xenon1t_detector_renamer(db_info)
                    #fixing the one minute offset bug in XENON1T by overwriting the dict_name from true
                    #Rucio locations such they are stored in the runDB
                    dict_name['date'] = i_data['location'].split(":")[0].split("_")[2]
                    dict_name['time'] = i_data['location'].split(":")[0].split("_")[3]
                    sr_run_selected = i_data['location'].split(":")[0].split("_")[1].replace("SR", "")


                #Fill in the templates:
                self.keyw.ResetTemplate()
                self.keyw.SetTemplate(dict_name)
                self.keyw.SetTemplate(db_info)
                self.keyw.SetTemplate({'hash': _hash, 'plugin': i_data['type']})
                self.keyw.SetTemplate({'science_run': sr_run_selected})

                rucio_template = self.keyw.CompleteTemplate(rucio_template)
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
                if rule != i_data['rse'] and len(rule) >= 1:
                    self.db.SetDataField(db_info['_id'], type=i_data['type'], host=i_data['host'], key='rse', value=rule)
                    helper.global_dictionary['logger'].Info("Updated location ({0}) for type {1} to {2}".format(i_data['location'], i_data['type'], rule))

                    #Since we update the runDB with new Rucio locations, I suppose we should update the status field
                    #just in case...
                    if self.db.GetDataField(db_info['_id'], type=i_data['type'], host=i_data['host'],
                                            key='status') != "transferred":
                        self.db.SetDataField(db_info['_id'], type=i_data['type'], host=i_data['host'], key='status',
                                             value="transferred")
                elif rule != i_data['rse'] and len(rule) == 0:
                    helper.global_dictionary['logger'].Info("No location ({0}) for type {1} to {2} found in Rucio".format(i_data['location'], i_data['type'], rule))
                    helper.global_dictionary['logger'].Info("Set for purge: status -> NoRucioEntry")
                    self.db.SetDataField(db_info['_id'], type=i_data['type'], host=i_data['host'], key='status',
                                         value="RucioClearance")
                    self.db.SetDataField(db_info['_id'], type=i_data['type'], host=i_data['host'], key='rse', value=rule)

                else:
                    helper.global_dictionary['logger'].Info("No update at location ({0}) for type {1} to {2}".format(i_data['location'], i_data['type'], rule))
