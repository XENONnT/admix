# -*- coding: utf-8 -*-
import json
import os
import shutil
from admix.helper import helper

from admix.interfaces.database import ConnectMongoDB
from admix.helper.decorator import Collector

# get Rucio imports done:
from admix.interfaces.rucio_dataformat import ConfigRucioDataFormat
from admix.interfaces.rucio_summoner import RucioSummoner
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.templater import Templater


@Collector
class PurgeMongoDB():
    """Class: PurgeMongoDB
    Purpose of this class is to purge physical data locations outside of Rucio which are registered to
    a meta database.

    Due to the complex file structure our data products the purge command became quite extensive in terms of
    selections to narrow down the datasets (depending on type, version (hash), host and location).

    PurgeMongoDB runs a purging process only one type of data (e.g. raw_records) at the same time. Multiple types
    are not supported (yet?).

    Therefore you can run this command with these inputs:

    admix PurgeMongoDB --admix-config /path/to/config/file.config
                       --select-run-times <date>_<time> (as single, multi or range)
                          or
                       --select-run-number XXXXX (as single or range)
                       --type data_type (e.g. raw_records)
                       --hash XXXXXXXXXX (10 character hash sum which represents a certain version of the data type
                       --force (Allows you enforce manual purge mode)
    Full example:
    admix PurgeMongoDB --once --admix-config  /home/bauermeister/Development/software/admix_config/host_config_dali_olddb.config --select-run-times 171225_0040-171225_0140 --type raw_records records --hash 7k65yaooed --force

    """
    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')

        # Init the runDB
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

    def run(self, *args, **kwargs):
        helper.global_dictionary['logger'].Info(f'Run task {self.__class__.__name__}')

        # Check for type pre-selection
        d_type = None
        d_hash = None
        if helper.global_dictionary.get("type") is None:
            helper.global_dictionary['logger'].Info(
                'Selected type of data to purge: {0}'.format(helper.global_dictionary.get("type")))
            helper.global_dictionary['logger'].Info("No data type is selected! Nothing to purge -> Finish here")
            return 1
        else:
            d_type = helper.global_dictionary.get("type")[0]

        if helper.global_dictionary.get("hash") is None:
            helper.global_dictionary['logger'].Info(
                'Selected type of data to purge: {0}'.format(helper.global_dictionary.get("hash")))
            helper.global_dictionary['logger'].Info(
                "No data hash (version) is selected! Nothing to purge -> Finish here")
            return 1
        else:
            d_hash = helper.global_dictionary.get("hash")[0]

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

        print("Purge Module")
        host_name = helper.get_hostname()
        hc_host = helper.get_hostconfig("host")  # Supposed to be the database entry to identfy locations

        # Get your collection of run numbers and run names what contain a certain host of choice
        # hint: You are allowed to get several plugins (types) and versions (hashsums) for the same
        #       dataset at a certain host.
        collection = self.db.GetHosts(hc_host, ts_beg, ts_end)

        # Run through the overview collection:
        for i_run in collection:

            # Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']

            # Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]

            print(db_info['name'], db_info['number'])

            # Data are supposed to have enough rucio copies:
            valid_rucio_copies = 0
            valid_rucio_rses = ""
            for i_data in db_info['data']:
                if i_data['host'] != "rucio-catalogue":
                    continue
                if i_data['type'] != d_type:
                    continue
                if d_hash not in i_data['location']:
                    continue
                if i_data['status'] != 'transferred':
                    continue
                valid_rucio_copies = sum([1 if i_rse.split(":")[1] == 'OK' else 0 for i_rse in i_data['rse']])
                valid_rucio_rses = [i_rse.split(":")[0] if i_rse.split(":")[1] == 'OK' else '' for i_rse in i_data['rse']]
            valid_rucio_rses = ', '.join(list(filter(None, valid_rucio_rses)))


            valid_disk_copies = 0
            for i_data in db_info['data']:
                if i_data['host'] == "rucio-catalogue":
                    continue
                if i_data['host'] == hc_host:
                    continue
                if i_data['type'] != d_type:
                    continue
                if d_hash not in i_data['location']:
                    continue
                if i_data['status'] != 'transferred':
                    continue
                valid_disk_copies += 1


            #Determine what to purge
            path_to_purge = None
            pHost = None
            pType = None
            pDict = None
            for i_data in db_info['data']:
                if i_data['host'] != hc_host:
                    continue
                if i_data['type'] != d_type:
                    continue
                if d_hash not in i_data['location']:
                    continue
                path_to_purge = i_data['location']
                pHost = i_data['host']
                pType = i_data['type']
                pDict = i_data

            print( helper.global_dictionary.get("force"))
            #purge without request is ok if:
            # - two valid rucio copies ("OK" for transfer)
            # - one valid rucio copy and one or more valid disk copies
            if ((valid_rucio_copies >= 2) or (valid_rucio_copies == 1 and valid_disk_copies >= 1)) and \
                ( helper.global_dictionary.get("force") == False):

                helper.global_dictionary['logger'].Info("Prepare to purge data path:")
                helper.global_dictionary['logger'].Info(f"> {path_to_purge}")

                #purge disk location
                purge_test = False
                try:
                    shutil.rmtree(path_to_purge)
                    helper.global_dictionary['logger'].Info("> Removed from disk [finished]")
                    purge_test = True
                except IOError as e:
                    helper.global_dictionary['logger'].Error("Unable to purge physical file")

                if purge_test == True:
                    #de-register from database
                    self.db.RemoveDatafield(db_info['_id'], pDict)
                    helper.global_dictionary['logger'].Info("> Removed from runDB [finished]")

            elif helper.global_dictionary.get("force") == True:
                helper.global_dictionary['logger'].Info("WARNING! You activated --force")
                helper.global_dictionary['logger'].Info("Confirm purge of datasets manually!")
                helper.global_dictionary['logger'].Info(f"Location: {path_to_purge}")
                helper.global_dictionary['logger'].Info(f"Type: {pType}")
                helper.global_dictionary['logger'].Info(f"Host: {pHost}")
                helper.global_dictionary['logger'].Info(f"Copies on other disks then {hc_host}: {valid_disk_copies}")
                helper.global_dictionary['logger'].Info(f"Copies in Rucio: {valid_rucio_copies}: {valid_rucio_rses}")

                f_purge = input("Do you with to purge that location (type in yes/no)? ")
                if f_purge == 'yes':
                    # purge disk location
                    purge_test = False
                    try:
                        shutil.rmtree(path_to_purge)
                        helper.global_dictionary['logger'].Info("> Removed from disk [finished]")
                        purge_test = True
                    except IOError as e:
                        helper.global_dictionary['logger'].Error("Unable to purge physical file")

                    if purge_test == True:
                        # de-register from database
                        self.db.RemoveDatafield(db_info['_id'], pDict)
                        helper.global_dictionary['logger'].Info("> Removed from runDB [finished]")

                else:
                    helper.global_dictionary['logger'].Info("You have chosen to not purge data")

            else:
                helper.global_dictionary['logger'].Info("Purging data is rejected!")
                helper.global_dictionary['logger'].Info(f"Location: {path_to_purge}")
                helper.global_dictionary['logger'].Info(f"Type: {pType}")
                helper.global_dictionary['logger'].Info(f"Host: {pHost}")
                helper.global_dictionary['logger'].Info(f"Copies on other disks then {hc_host}: {valid_disk_copies}")
                helper.global_dictionary['logger'].Info(f"Copies in Rucio: {valid_rucio_copies}: {valid_rucio_rses}")


        return 0
