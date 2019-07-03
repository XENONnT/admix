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
class InitTransfersMongoDB():

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
        collection = self.db.GetDestination(ts_beg, ts_end)

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

                #get the destination from DB:
                origin_dest = None
                if 'destination' in i_data:
                    origin_dest = i_data['destination']

                #build a destination from host=origin (=rucio-catalogue)
                dest = self.destination.EvalDestination(host="rucio-catalogue",
                                                        origin=i_data['host'],
                                                        destination=origin_dest)

                if len(dest) == 0:
                    helper.global_dictionary['logger'].Info("No destination specified! Skip")
                    continue

                #At this stage we know of an existing destination:
                rc_scope = i_data['location'].split(":")[0]
                rc_dname = i_data['location'].split(":")[1]

                rucio_template = self.rc_reader.GetPlugin(i_data['type'])
                rucio_template_sorted = [key for key in sorted(rucio_template.keys())]

                # Fill the key word class with information beforehand:
                self.keyw.ResetTemplate()
                #self.keyw.SetTemplate(template_info)
                self.keyw.SetTemplate(db_info)
                self.keyw.SetTemplate({'plugin': i_data['type'],
                                       "hash": i_data['location'].split(":")[-1].split("-")[-1]
                                       })
                self.keyw.SetTemplate(dict_name)
                self.keyw.SetTemplate({'science_run': helper.get_science_run(db_info['start'])})

                rucio_template = self.keyw.CompleteTemplate(rucio_template)

                helper.global_dictionary['logger'].Info('Type {0}'.format(i_data['type']))
                helper.global_dictionary['logger'].Debug('Run is at host: {0}'.format(i_data['host']))
                helper.global_dictionary['logger'].Debug('aDMIX runs at host: {0}'.format(helper.get_hostconfig('host')))

                #Begin to work through the new/updated rucio destinations:
                for i_dest in dest:
                    #print(i_dest)
                    rse_rules = ["{protocol}:{rse}:{lifetime}".format(protocol=i_dest['protocol'],
                                                                     rse=i_dest['rse'],
                                                                     lifetime=i_dest['lifetime'])]

                    try:
                        rule = self.rc.GetRule(upload_structure=rucio_template, rse=i_dest['rse'])
                    except:
                        helper.global_dictionary['logger'].Error("No rule transfer rule applied for {0}".format(i_dest['rse']))
                        continue
                    #1) Rule does not exists -> Create it, delete destination
                    if rule['state'] == "Unkown" and rule['rse'] == None:
                        try:
                            helper.global_dictionary['logger'].Info('Add a Rucio transfer rule')
                            self.rc.AddRules(upload_structure=rucio_template, rse_rules= rse_rules)
                        except:
                            helper.global_dictionary['logger'].Error('Can not create Rucio transfer rule')
                    #2) Rule exists but lifetime is different -> Update lifetime, delete destination
                    if rule['state'] == "OK" and i_dest['lifetime'] == 'None':
                        helper.global_dictionary['logger'].Info('No need to do anything!')
                    #3) Rule does exists without lifetime changes -> delete destination
                    if rule['state'] == "OK" and i_dest['lifetime'] != 'None':
                        try:
                            helper.global_dictionary['logger'].Info('Update Rucio transfer rule')
                            self.rc.UpdateRules(upload_structure=rucio_template, rse_rules= rse_rules)
                        except:
                            helper.global_dictionary['logger'].Error('Can not update a transfer rule')


                #update runDB after rules are updated/added:
                rucio_rules = self.rc.ListDidRules(rucio_template)

                rse_to_db = []
                for i_rule in rucio_rules:
                    if i_rule['expires_at'] == None:
                        dt = None
                    else:
                        dt = i_rule['expires_at'].strftime("%Y-%m-%d-%H:%M:%S")
                    db_field = "{rse}:{state}:{lifetime}".format(rse=i_rule['rse_expression'],
                                                                 state=i_rule['state'],
                                                                 lifetime=dt)
                    rse_to_db.append(db_field)

                helper.global_dictionary['logger'].Info(f'Update database with {rse_to_db}')
                self.db.SetDataField(db_info['_id'], type=i_data['type'],
                                                     host='rucio-catalogue',
                                                     key='rse',
                                                     value=rse_to_db)

                #Last step is to evaluate if InitTransfersWithMongoDB was able to init new Rucio transfer rules
                #extract a list of required destinations:
                t_dest = [ i['rse'] for i in dest ]

                for i_rule_found in rse_to_db:
                    #information about rules which are found in Rucio about the DID
                    t_rse    = i_rule_found.split(":")[0]


                    if t_rse in t_dest:
                        dest[:] = [f"{i_dict.get('protocol')}:{i_dict.get('rse')}:{i_dict.get('lifetime')}" for
                                       i_dict in dest if i_dict.get('rse') != t_rse]

                        helper.global_dictionary['logger'].Info(f'Update database with destinations {dest}')

                        self.db.SetDataField(db_info['_id'], type=i_data['type'],
                                             host='rucio-catalogue',
                                             key='destination',
                                             value=dest)
                    else:
                        helper.global_dictionary['logger'].Info(f'Database destinations are still: {dest}')

