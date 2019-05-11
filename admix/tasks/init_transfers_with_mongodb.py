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
        print("inittransfermongodb")
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

                helper.global_dictionary['logger'].Info('Run number/type: {0}/{1}'.format(r_number, i_data['type']))
                helper.global_dictionary['logger'].Debug('Run is at host: {0}'.format(i_data['host']))
                helper.global_dictionary['logger'].Debug('aDMIX runs at host: {0}'.format(helper.get_hostconfig('host')))

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

                print("rucio template after filling:")
                print("-", rucio_template)


                #Begin to work through the new/updated rucio destinations:
                for i_dest in dest:
                    #print(i_dest)
                    rse_rules = ["{protocol}:{rse}:{lifetime}".format(protocol=i_dest['protocol'],
                                                                     rse=i_dest['rse'],
                                                                     lifetime=i_dest['lifetime'])]
                    print(rse_rules)
                    rule = self.rc.GetRule(upload_structure=rucio_template, rse=i_dest['rse'])


                    #1) Rule does not exists -> Create it, delete destination
                    if rule['state'] == "Unkown" and rule['rse'] == None:
                        print("add transfer rule")
                        self.rc.AddRules(upload_structure=rucio_template, rse_rules= rse_rules)
                    #2) Rule exists but lifetime is different -> Update lifetime, delete destination
                    if rule['state'] == "OK" and i_dest['lifetime'] == 'None':
                        print("everything is ok")
                    #3) Rule does exists without lifetime changes -> delete destination
                    if rule['state'] == "OK" and i_dest['lifetime'] != 'None':
                        print("update transfer rules")
                        self.rc.UpdateRules(upload_structure=rucio_template, rse_rules= rse_rules)

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

                print(rse_to_db)
                self.db.SetDataField(db_info['_id'], type=i_data['type'],
                                                     host='rucio-catalogue',
                                                     key='destination',
                                                     value=rse_to_db)


    def __del__(self):
        print( 'Upload mongoDB stops')
