# -*- coding: utf-8 -*-
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
class ClearTransfersMongoDB():

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

        # Init the Rucio data format evaluator in three steps:
        self.rc_reader = ConfigRucioDataFormat()
        self.rc_reader.Config(helper.get_hostconfig('rucio_template'))

        # This class will evaluate your destinations:
        self.destination = Destination()

        # Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config(helper.get_hostconfig()['template'])

        # Init a class to handle keyword strings:
        self.keyw = Keyword()

        # Init Rucio for later uploads and handling:
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
            # Evaluate terminal input for run number assumption (terminal input == true)
            true_nb_beg, true_nb_end = helper.eval_run_numbers(helper.global_dictionary.get('run_numbers'),
                                                               self.run_nb_min,
                                                               self.run_nb_max)
            # Get the timestamps from the run numbers:
            ts_beg = self.db.FindTimeStamp('number', int(true_nb_beg))
            ts_end = self.db.FindTimeStamp('number', int(true_nb_end))

        elif helper.global_dictionary.get('run_timestamps') != None:
            # Evaluate terminal input for run name assumption
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
            helper.global_dictionary['logger'].Error(
                "Check for your input arguments (--select-run-number or --select-run-time")
            exit(1)
            # exection

        # After we know the times:
        helper.global_dictionary['logger'].Info(f"Run between {ts_beg} and {ts_end}")

        #Get your collection of run numbers and run names
        collection = self.db.GetClearance(ts_beg, ts_end)

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

            #REMOVE AND CHECK FOR UPLOAD DETECTORS (-> config files)
            #ToDo: Make it work with XENONnT later
            if db_info['detector'] != 'tpc':
                continue

            for i_data in db_info['data']:

                #Skip everything what is not host rucio-catalogue
                if i_data.get('host') != "rucio-catalogue":
                    continue
                #Skip everything what has not the requested clearance status
                if i_data.get('status') != "RucioClearance":
                    continue

                #remove the field from the database:
                self.db.RemoveDatafield(db_info['_id'], i_data)

                helper.global_dictionary['logger'].Info("Remove:")
                helper.global_dictionary['logger'].Info(f"Status: {i_data.get('status')}")
                helper.global_dictionary['logger'].Info(f"Location: {i_data.get('location')}")
                helper.global_dictionary['logger'].Info(f"Type: {i_data.get('type')}")
                helper.global_dictionary['logger'].Info(f"for run {r_number}/{db_info['name']} from runDB")

