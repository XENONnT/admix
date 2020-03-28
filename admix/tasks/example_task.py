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

@Collector
class RunExampleTask():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')

        """Use this function to init all important functions, classes and setup variables
        for the actual task which is later executed by run()
        """

        #Init the runDB
        self.db = ConnectMongoDB()
        self.db.Connect()

        #We want the first and the last run:
        self.gboundary = self.db.GetBoundary()
        self.run_nb_min = self.gboundary['min_number']
        self.run_nb_max = self.gboundary['max_number']
        self.run_ts_min = self.gboundary['min_start_time']
        self.run_ts_max = self.gboundary['max_start_time']

        #Init the Rucio data format evaluator in three steps:
        #self.rc_reader = ConfigRucioDataFormat()
        #self.rc_reader.Config(helper.get_hostconfig('rucio_template'))

        #This class will evaluate your destinations:
        self.destination = Destination()

        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config(helper.get_hostconfig()['template'])

        #Init a class to handle keyword strings:
        self.keyw = Keyword()

        #Init Rucio for later uploads and handling:
        #self.rc = RucioSummoner(helper.get_hostconfig("rucio_backend"))
        #self.rc.SetRucioAccount(helper.get_hostconfig('rucio_account'))
        #self.rc.SetConfigPath(helper.get_hostconfig("rucio_cli"))
        #self.rc.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
        #self.rc.SetHost(helper.get_hostconfig('host'))
        #self.rc.ConfigHost()

    def run(self,*args, **kwargs):
        """Execute your task

        Obs: To simply logging, there is a logger class whose object is found in a global
             dictionary:
             helper.global_dictionary['logger'].Info()
             helper.global_dictionary['logger'].Warning()
             helper.global_dictionary['logger'].Error()
             helper.global_dictionary['logger'].Debug()

        :param args: Choose additional parameter (list) on purpose
        :param kwargs: Choose additional parameter (dict) on purpse
        :return: 0
        """
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')



        #This part will help you to evaluate your command line input.
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

        #After we know the times:
        helper.global_dictionary['logger'].Info(f"Run between {ts_beg} and {ts_end}")

        #Here begins your task:
        ...

        print(ts_beg,ts_end)

        #Get your collection of run numbers and run names
        collection = self.db.GetDestinationTest(ts_beg, ts_end)

        #print(collection)

        #Run through the overview collection:
        for i_run in collection:

            #Extract run number and name from overview collection
#            r_name = i_run['name']
#            r_number = i_run['number']
            print(i_run)


        return 0

    def __del__(self):
        """Use this function to clean up your class settings if necessary"""

#        print('dummy stop')
        pass
