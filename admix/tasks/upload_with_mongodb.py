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
class UploadMongoDB():

    def __init__(self):
        pass

    def init(self):
        helper.global_dictionary['logger'].Info(f'Init task {self.__class__.__name__}')

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

        #After we know the times:
        helper.global_dictionary['logger'].Info(f"Run between {ts_beg} and {ts_end}")

        #Get your collection of run numbers and run names
        collection = self.db.GetDestination(ts_beg, ts_end)

        print(collection)

        #Run through the overview collection:
        for i_run in collection:

            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #print("test")

            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]

            #REMOVE AND CHECK FOR UPLOAD DETECTORS (-> config files)
            #ToDo: Make it work with XENONnT later
            if db_info['detector'] != 'tpc':
                continue

            helper.global_dictionary['logger'].Info(f"Run: {r_name} / {r_number}")

            for i_data in db_info['data']:

                #get the destination from DB:
                origin_dest = None
                if 'destination' in i_data:
                    origin_dest = i_data['destination']

                dest = self.destination.EvalDestination(host=helper.get_hostconfig('host'),
                                                        origin=i_data['host'],
                                                        destination=origin_dest)

                if len(dest) == 0:
                    helper.global_dictionary['logger'].Debug("No destination specified! Skip")
                    continue

                helper.global_dictionary['logger'].Info('Type {0}'.format(i_data['type']))
                helper.global_dictionary['logger'].Debug('Run is at host: {0}'.format(i_data['host']))
                helper.global_dictionary['logger'].Debug('aDMIX runs at host: {0}'.format(helper.get_hostconfig('host')))

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

                helper.global_dictionary['logger'].Debug("Evaluate origin information:")
                helper.global_dictionary['logger'].Debug(f" > Origin Host: {origin_host}")
                helper.global_dictionary['logger'].Debug(f" > Origin Status: {origin_status}")

                #Extract the template information according the pre-defined physical file structure:
                template_info = self.exp_temp.GetTemplateEval(plugin=origin_type,
                                                              host=origin_host,
                                                              string=origin_location)


                #print("template info:")
                #print(template_info)

                #print("rucio plugin info:")
                #print(":", self.rc_reader.GetPlugin(origin_type, reset=True))
                #print(":", self.rc_reader.GetTypes())

                #Evaluate the Rucio name template according the plugin which is requested:
                # rucio_template: Holds the unsorted Rucio container/dataset/file structure in levels
                # rucio_template_sorted: Holds the sorted levels (L0, L1, L2,...)
                rucio_template = self.rc_reader.GetPlugin(origin_type, reset=True)
                rucio_template_sorted = [key for key in sorted(rucio_template.keys())]


                #Fill the key word class with information beforehand:
                self.keyw.ResetTemplate()
                self.keyw.SetTemplate(template_info)
                self.keyw.SetTemplate(db_info)
                self.keyw.SetTemplate({'science_run': helper.get_science_run(db_info['start'])})

                rucio_template = self.keyw.CompleteTemplate(rucio_template)

                #collect all files from the directory and do pre checks:
                if os.path.isdir(origin_location) == False:
                    helper.global_dictionary['logger'].Error(f"Folder {origin_location} does not exists")
                    continue

                #load files:
                r_path, r_folder, r_files = helper.read_folder(origin_location)

                #check if files are there:
                if len(r_files) == 0:
                    helper.global_dictionary['logger'].Error(f"There are no files in the folder {origin_location}")
                    continue

                #check for zero sizes files before hand:
                file_checks = False
                for i_file in r_files:
                    f_size = os.path.getsize( os.path.join(origin_location, i_file) )
                    #print(i_file, f_size)
                    if f_size == 0:
                        helper.global_dictionary['logger'].Error(f"File {i_file} is 0 bytes")
                        helper.global_dictionary['logger'].Error(f"Exclude directory {origin_location} from upload")
                        file_checks=True
                        continue
                    if '_temp' in i_file:
                        #avoid to upload temp. incomplete files (ToDo need to checked with Strax!)
                        helper.global_dictionary['logger'].Error(f"Identified _temp extension in file name {i_file}")
                        file_checks=True
                        continue
                    if template_info['plugin'] not in i_file or template_info['hash'] not in i_file:
                        #avoid to upload wrong file names. (ToDo: Maybe we can improve in this test but works for not)
                        helper.global_dictionary['logger'].Error(f"Plugin name and hash is not recognized in file name {i_file}")
                        file_checks=True
                        continue

                if file_checks == True:
                    helper.global_dictionary['logger'].Info(f"File checks not successful!")
                    continue

                #Check if rule exists in Rucio:
                rule_status = self.rc.CheckRule(upload_structure=rucio_template, rse=rse0)
                #verify the locations with the rucio catalogue
                #Hint: the outcome of this check does not skip uploading! (missing files on disk):
                verify_location, _, _ = self.rc.VerifyLocations(upload_structure=rucio_template, upload_path=origin_location, checksum_test=False)
                #check for a database entry:
                db_dest_status = self.db.StatusDataField(db_info['_id'], type=origin_type, host=ptr0)

                helper.global_dictionary['logger'].Info(f"Rucio rule status is: {rule_status}")
                helper.global_dictionary['logger'].Info(f"Rucio vs. Disk file verfication is: {verify_location}")
                helper.global_dictionary['logger'].Info(f"Database status is: {db_dest_status}")

                #logging the filled template for debugging:
                for tmpl_key, tmpl_val in rucio_template.items():
                    helper.global_dictionary['logger'].Debug(f"Level: {tmpl_key}")
                    for inn_key, inn_val in tmpl_val.items():
                        helper.global_dictionary['logger'].Debug(f"  - {inn_key} : {inn_val}")

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
                    helper.global_dictionary['logger'].Info("A Rucio rule exits already. We skip here!")

                elif db_dest_status == True:

                    if self.db.GetDataField(db_info['_id'],
                                            type=origin_type,
                                            host=ptr0, key='status') == 'transferring' or \
                       self.db.GetDataField(db_info['_id'],
                                             type=origin_type,
                                             host=ptr0, key='status') == 'transferred' or \
                        self.db.GetDataField(db_info['_id'],
                                                 type=origin_type,
                                                 host=ptr0, key='status') == 'RucioClearance':

                        skip_upload = True
                    elif self.db.GetDataField(db_info['_id'],
                                            type=origin_type,
                                            host=ptr0, key='status') == 'RSEreupload':
                        skip_upload = False

                if helper.global_dictionary['force'] == True:
                    helper.global_dictionary['logger'].Info("Enable forced upload to Rucio")
                    skip_upload = False


                #elif db_dest_status == True and self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status') == 'RSEreupload':
                #    #There is a database status found in RSEreupload state: Reset it and continoue with uploading:
                #    skip_upload = False
                #    #self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='transferring')
                #    #self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='location', value='n/a')
                #elif db_dest_status == True and len(self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse'))>0:
                #    #if there exists a list of RSEs for the rucio-catalogue entry which contains the upload RSE we skip
                #    list_rse_is = self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse')
                #    for i_rse_is in list_rse_is:
                #        print("-", i_rse_is)
                #        if i_rse_is.split(":")[0]==rse0 and i_rse_is.split(":")[1]=='OK':
                #            skip_upload = True

                if skip_upload==True:
                    tmp_status = self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status')
                    tmp_rse = self.db.GetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse')
                    helper.global_dictionary['logger'].Info("We skip uploading here because:\n "\
                                                            f"Database status is {tmp_status} and "\
                                                            f"Database RSE is {tmp_rse}. "\
                                                            "We do not want to interfere here...")
                    continue



                #if the pre checks are ok we can upload:
                upload_result = 1

                try:
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='transferring')
                    helper.global_dictionary['logger'].Info("Start Rucio upload...")
                    upload_result = self.rc.Upload(upload_structure=rucio_template,
                                                   upload_path=origin_location,
                                                   rse=rse0,
                                                   rse_lifetime=rlt0)
                    helper.global_dictionary['logger'].Info("Rucio upload finished")
                except:
                    helper.global_dictionary['logger'].Error("Rucio upload failed")
                    #We can skip everything after the rucio upload failed
                    #Just set the Rucio database entry into RSEreupload to allow admix
                    #to try it again later
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='RSEreupload')
                    continue

                #Intend that the try/execpt run through well but there was a problem with the upload/rule setting
                #of the DID. In that case we do not want to contioue:
                if upload_result == 1:
                    helper.global_dictionary['logger'].Error("Rucio upload may have failed. Check for DID")
                    #continue


                #Verify if all files are uploaded (verify_location == True is enough)
                verify_location, _, _ = self.rc.VerifyLocations(upload_structure=rucio_template, upload_path=origin_location, checksum_test=False)
                rucio_rule = self.rc.GetRule(upload_structure=rucio_template, rse=rse0)

                ##update after initial upload:
                if verify_location == True and rucio_rule['state'] == 'OK':
                    helper.global_dictionary['logger'].Info("Rucio rule status after upload is OK")

                    #get & set rse information and location:
                    rse_to_db = ["{rse}:{state}:{lifetime}".format(rse=rse0, state=rucio_rule['state'], lifetime=rucio_rule['expires'])]
                    rc_location = rucio_template[rucio_template_sorted[-1]]['did']

                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse', value=rse_to_db)
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='location', value=rc_location)
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='transferred')

                    helper.global_dictionary['logger'].Info(f"RSE information: {rse_to_db}")
                    helper.global_dictionary['logger'].Info(f"DID location: {rc_location}")
                    helper.global_dictionary['logger'].Info("Switch DB status to transferred")


                    #if everything was ok we can dump the metadata.json to the runDB:
                    meta_data_file = None
                    for i_file in r_files:
                        if 'metadata.json' in i_file:
                            meta_data_file = os.path.join(origin_location, i_file)
                            break
                    if os.path.exists(meta_data_file):
                        md = json.load(open(meta_data_file))
                        helper.global_dictionary['logger'].Info(f"Metadata file {meta_data_file} found and loaded")
                    else:
                        helper.global_dictionary['logger'].Error(f"Metadata file {meta_data_file} not found!")
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
                                try:
                                    helper.global_dictionary['logger'].Info(f"Apply {rule_list} to Rucio DID")
                                    self.rc.AddRules(upload_structure=rucio_template, rse_rules=rule_list)
                                except:
                                    helper.global_dictionary['logger'].Error(f"Rule {rule_list} can not be applied!")

                    #if we have a complete uploaded data set and further rules initiated
                    #we can update the destination list:
                    #create an empty list of rule status messages:
                    rucio_rule_status = []

                    #get rule for the uploaded location:
                    try:
                        rucio_rule_rse0 = self.rc.GetRule(upload_structure=rucio_template, rse=rse0)
                        rucio_rule_status.append("{rse}:{state}:{lifetime}".format(rse=rse0,
                                                                                   state=rucio_rule_rse0['state'],
                                                                                   lifetime=rucio_rule_rse0['expires']))
                    except:
                        helper.global_dictionary['logger'].Error(f"Get Rucio rule for {rse0} failed"\
                                                                 f"We skip further Rucio rule requests"\
                                                                 f"and leave it to the admix rule updater")
                    #collect more rule status information about the
                    for i_dest in dest:
                        if i_dest['upload'] == False:
                            ptr1 = i_dest['protocol']
                            rlt1 = i_dest['lifetime']
                            rse1 = i_dest['rse']

                            #apply further rucio rules
                            if ptr1 == 'rucio-catalogue':
                                try:
                                    rucio_rule_rse1 = self.rc.GetRule(upload_structure=rucio_template, rse=rse1)
                                    rucio_rule_status.append("{rse}:{state}:{lifetime}".format(rse=rse1,
                                                                                           state=rucio_rule_rse1['state'],
                                                                                           lifetime=rucio_rule_rse1['expires']))
                                except:
                                    helper.global_dictionary['logger'].Error(f"Get Rucio rule for {rse0} failed "\
                                                                             f"We skip further Rucio rule requests "\
                                                                             f"and leave it to the admix rule updater")

                    #delete the destination if everything seems ok:
                    if rucio_rule_rse0['state'] == 'OK' and len(rucio_rule_status) == len(dest):
                        helper.global_dictionary['logger'].Info("Upload and transfer rule setting was SUCCESSFUL\n}"
                                                                "-> Empty the destination field at the host")
                        self.db.SetDataField(db_info['_id'], type=origin_type, host=origin_host, key='destination', value=[])

                    #But also update the data field based on the latest rucio information:
                    helper.global_dictionary['logger'].Info("Finally: Update Rucio entry in the database by:")
                    for i_rule in rucio_rule_status:
                        helper.global_dictionary['logger'].Info(f" > Rule: {i_rule}")

                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse', value=rucio_rule_status)

                else:
                    #rse_to_db = ["{rse}:{state}:{lifetime}".format(rse=rse0,
                    #                                               state=rucio_rule['state'],
                    #                                               lifetime=rucio_rule['expires'])]
                    #self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='rse', value=rse_to_db)
                    helper.global_dictionary['logger'].Error("Something went wrong during the upload and we can not"\
                                                             "verify the Rucio transfer rule status"\
                                                             "-> Set to RSEreupload in the database.")
                    self.db.SetDataField(db_info['_id'], type=origin_type, host=ptr0, key='status', value='RSEreupload')

        return 0


    def __del__(self):
        pass
