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
class DownloadMongoDB():

    def __init__(self):
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

        #Specifiy the download path:
        if 'destination' not in helper.global_dictionary:
            dw_path = "./"
        else:
            dw_path = helper.global_dictionary['destination']
        if 'rse' not in helper.global_dictionary:
            dw_rse = None
        else:
            dw_rse =  helper.global_dictionary['rse']

        print("Dw path:", dw_path)
        print("Dw from RSE", dw_rse)

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
                #origin_dest = None
                #if 'destination' in i_data:
                #    origin_dest = i_data['destination']

                #print("destination:", origin_dest)
                #print(i_data)
                origin_dest = "file:./:None"

                dest = self.destination.EvalDestination(host=helper.get_hostconfig('host'),
                                                        origin=i_data['host'],
                                                        destination=origin_dest)
                print(dest)
                #if len(dest) == 0:
                #    helper.global_dictionary['logger'].Info("No destination specified! Skip")
                #    continue


                continue
                #reactivate if this is specified in the config file
                #if helper.global_dictionary['plugin_type'] != None and helper.global_dictionary['plugin_type'] != i_date['type']:
                    #continue

                ptr0 = None
                rlt0 = None
                rse0 = None
                for i_dest in dest:
                    if i_dest['upload'] == True:
                        ptr0 = i_dest['protocol']
                        rlt0 = i_dest['lifetime']
                        if rlt0 == "None":
                            rlt0 = None
                        rse0 = i_dest['rse']
                helper.global_dictionary['logger'].Debug("Evaluated destination for upload:")
                helper.global_dictionary['logger'].Debug(f" > Protocol: {ptr0}")
                helper.global_dictionary['logger'].Debug(f" > Lifetime: {rlt0}")
                helper.global_dictionary['logger'].Debug(f" > RSE: {rse0}")

                #pre infos:
                origin_type     = i_data['type'] #This is the plugin name which is gonna be handled (uploaded)
                origin_location = i_data['location'] #This holds the physical location of the plugin folder
                origin_host     = i_data['host']
                origin_status   = i_data['status']

                helper.global_dictionary['logger'].Info("Evaluate origin information:")
                helper.global_dictionary['logger'].Debug(" (origin means from where data are uploaded)")
                helper.global_dictionary['logger'].Info(f" > Origin Type: {origin_type}")
                helper.global_dictionary['logger'].Info(f" > Origin Location: {origin_location}")
                helper.global_dictionary['logger'].Debug(f" > Origin Host: {origin_host}")
                helper.global_dictionary['logger'].Debug(f" > Origin Status: {origin_status}")

                #Extract the template information according the pre-defined physical file structure:
                template_info = self.exp_temp.GetTemplateEval(plugin=origin_type,
                                                              host=origin_host,
                                                              string=origin_location)


                print("template info:")
                print(template_info)

                print("rucio plugin info:")
                print(":", self.rc_reader.GetPlugin(origin_type))
                print(":", self.rc_reader.GetTypes())

                #Evaluate the Rucio name template according the plugin which is requested:
                # rucio_template: Holds the unsorted Rucio container/dataset/file structure in levels
                # rucio_template_sorted: Holds the sorted levels (L0, L1, L2,...)
                rucio_template = self.rc_reader.GetPlugin(origin_type)
                rucio_template_sorted = [key for key in sorted(rucio_template.keys())]


                #Fill the key word class with information beforehand:
                self.keyw.ResetTemplate()
                self.keyw.SetTemplate(template_info)
                self.keyw.SetTemplate(db_info)
                self.keyw.SetTemplate({'science_run': helper.get_science_run(db_info['start'])})

                rucio_template = self.keyw.CompleteTemplate(rucio_template)

                print("rucio template after filling:")
                print("-", rucio_template)

                #collect all files from the directory and do pre checks:
                if os.path.isdir(origin_location) == False:
                    helper.global_dictionary['logger'].Warning(f"Folder {origin_location} does not exists, stop here")
                    continue

                #load files:
                r_path, r_folder, r_files = helper.read_folder(origin_location)

                #check if files are there:
                if len(r_files) == 0:
                    helper.global_dictionary['logger'].Warning("--> There are no files in the folder")

                #check for zero sizes files before hand:
                file_checks = False
                for i_file in r_files:
                    f_size = os.path.getsize( os.path.join(origin_location, i_file) )
                    #print(i_file, f_size)
                    if f_size == 0:
                        helper.global_dictionary['logger'].Warning(f"--> file {i_file} is 0 bytes")
                        helper.global_dictionary['logger'].Warning(f"--> Exclude directory {origin_location} from upload")
                        file_checks=True
                        continue
                    if '_temp' in i_file:
                        #avoid to upload temp. incomplete files (ToDo need to checked with Strax!)
                        helper.global_dictionary['logger'].Warning(f"Identified _temp extension in file name {i_file}")
                        file_checks=True
                        continue
                    if template_info['plugin'] not in i_file or template_info['hash'] not in i_file:
                        #avoid to upload wrong file names. (ToDo: Maybe we can improve in this test but works for not)
                        helper.global_dictionary['logger'].Warning(f"Plugin name and hash is not recognized in file name {i_file}")
                        file_checks=True
                        continue

                if file_checks == True:
                    continue

                #Check if rule exists in Rucio:
                rule_status = self.rc.CheckRule(upload_structure=rucio_template, rse=rse0)
                #verify the locations with rucio:
                verify_location = self.rc.VerifyLocations(upload_structure=rucio_template, upload_path=origin_location, checksum_test=False)
                #check for a database entry:
                db_dest_status = self.db.StatusDataField(db_info['_id'], type=origin_type, host=ptr0)

                print("Check:")
                print("Rucio Rules Status", rule_status)
                print("Rucio verify location", verify_location)
                print("DB status: ", db_dest_status)
                print("-----------------------")
                for tmpl_key, tmpl_val in rucio_template.items():
                    print("level", tmpl_key, tmpl_val)
                print("-----------------------")

#                p = self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status')
#                print(p, origin_type)
#                #self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='RSEreupload')
#                continue

                skip_upload = False
                if db_dest_status == False:
                    #Create a new dictionary in the data field for the plugin/host combination
                    new_data_dict={}
                    new_data_dict['location'] = "n/a"
                    new_data_dict['status'] = "transferring"
                    new_data_dict['host'] = ptr0
                    new_data_dict['type'] = origin_type
                    new_data_dict['meta'] = None
                    if ptr0 == 'rucio-catalogue':
                        new_data_dict['rse'] = []
                        new_data_dict['destination'] = []

                    self.db.AddDatafield(db_info['_id'], new_data_dict)

                elif rule_status == 'OK':
                    #if there is a rucio rule we can skip here
                    skip_upload = True
                elif db_dest_status == True and self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status') == 'transferring':
                    skip_upload = True
                elif db_dest_status == True and self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status') == 'transferred':
                    skip_upload = True
                elif db_dest_status == True and self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status') == 'RSEreupload':
                    #There is a database status found in RSEreupload state: Reset it and continoue with uploading:
                    pass
                    #self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='transferring')
                    #self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='location', value='n/a')
                elif db_dest_status == True and len(self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse'))>0:
                    #if there exists a list of RSEs for the rucio-catalogue entry which contains the upload RSE we skip
                    list_rse_is = self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse')
                    for i_rse_is in list_rse_is:
                        print("-", i_rse_is)
                        if i_rse_is.split(":")[0]==rse0 and i_rse_is.split(":")[1]=='OK':
                            skip_upload = True

                print("skip:", skip_upload)
                print("-- origin_type: ", origin_type)
                print("-- DB status:", self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status'))
                print("-- DB RSE: ", self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse'))

                if skip_upload==True:
                    continue

                #if the pre checks are ok we can upload:
                self.rc.Upload(upload_structure=rucio_template, upload_path=origin_location, rse=rse0, rse_lifetime=rlt0)
#[File replicas states successfully updated]
                verify_location = self.rc.VerifyLocations(upload_structure=rucio_template, upload_path=origin_location, checksum_test=False)
                rucio_rule = self.rc.GetRule(upload_structure=rucio_template, rse=rse0)

                ##update after initial upload:
                if verify_location == True and rucio_rule['state'] == 'OK':
                    rse_to_db = ["{rse}:{state}:{lifetime}".format(rse=rse0, state=rucio_rule['state'], lifetime=rucio_rule['expires'])]
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse', value=rse_to_db)
                    rc_location = rucio_template[rucio_template_sorted[-1]]['did']
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='location', value=rc_location)
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='transferred')

                    #if everything was ok we can dump the metadata.json to the runDB:
                    meta_data_file = None
                    for i_file in r_files:
                        if 'metadata.json' in i_file:
                            meta_data_file = os.path.join(origin_location, i_file)
                            break
                    if os.path.exists(meta_data_file):
                        md = json.load(open(meta_data_file))
                    else:
                        md = {}
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='meta', value=md)


                    #Clean up: Remove destination field and add some rules:
                    #remove destination entry:
                    #1 find out if other destinations are on the list:
                    for i_dest in dest:
                        if i_dest['upload'] == False:
                            ptr1 = i_dest['protocol']
                            rlt1 = i_dest['lifetime']
                            rse1 = i_dest['rse']

                            #apply further rucio rules
                            if ptr1 == 'rucio-catalogue':
                                rule_list = ["{ptr}:{rse}:{rlt}".format(ptr=ptr1,
                                                                        rse=rse1,
                                                                        rlt=rlt1)]
                                print(rule_list)
                                self.rc.AddRules(upload_structure=rucio_template, rse_rules=rule_list)

                    #if we have a complete uploaded data set and further rules initiated
                    #we can reset the destintaion list
                    rucio_rule_status = []
                    rucio_rule_rse0 = self.rc.GetRule(upload_structure=rucio_template, rse=rse0)
                    rucio_rule_status.append("{rse}:{state}:{lifetime}".format(rse=rse0, state=rucio_rule_rse0['state'], lifetime=rucio_rule_rse0['expires']))
                    for i_dest in dest:
                        if i_dest['upload'] == False:
                            ptr1 = i_dest['protocol']
                            rlt1 = i_dest['lifetime']
                            rse1 = i_dest['rse']

                            #apply further rucio rules
                            if ptr1 == 'rucio-catalogue':
                                rucio_rule_rse1 = self.rc.GetRule(upload_structure=rucio_template, rse=rse1)
                                rucio_rule_status.append("{rse}:{state}:{lifetime}".format(rse=rse1, state=rucio_rule_rse1['state'], lifetime=rucio_rule_rse1['expires']))
                    print(rucio_rule_status)
                    #delete the destination if everything seems ok:
                    if rucio_rule_rse0['state'] == 'OK' and len(rucio_rule_status) == len(dest):
                        print("Destination request seems to be fulfilled")
                        print(rucio_rule_status)
                        self.db.SetDataField(db_info['_id'], type=origin_type, host=origin_host, key='destination', value=[])

                    #But also update the data field based on the latest rucio information:
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse', value=rucio_rule_status)
                    #------Goes to memberfunctions laters

                else:
                    rse_to_db = ["{rse}:{state}:{lifetime}".format(rse=rse0, state=rucio_rule['state'], lifetime=rucio_rule['expires'])]
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse', value=rse_to_db)
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='RSEreupload')

                print("TTTTTTJJJJ")
                print(rucio_rule)
                print(verify_location)
                print(db_dest_status)
                self.db.ShowDataField(db_info['_id'], type=origin_type, host=ptr0)


                for i_dest in dest:
                    if i_dest['upload'] == False:
                        ptr1 = i_dest['protocol']
                        rlt1 = i_dest['lifetime']
                        rse1 = i_dest['rse']
                        print(ptr1, rlt1, rse1)



    def __del__(self):
        pass
#        print( 'Download mongoDB stops')
