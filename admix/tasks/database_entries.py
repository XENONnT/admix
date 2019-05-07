# -*- coding: utf-8 -*-

#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

import os

import admix.helper.helper as helper
from admix.interfaces.database import ConnectMongoDB
from admix.interfaces.destination import Destination
from admix.interfaces.keyword import Keyword
from admix.interfaces.rucio_summoner import ConfigRucioDataFormat, TransferRucio
from admix.interfaces.templater import Templater


class database_entries():

    def __init__(self):
        print('Database entries check starts')

    def init(self):
        self.db = ConnectMongoDB()
        self.db.Connect()

        #This class will evaluate your destinations:
        self.destination = Destination()

        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config(helper.get_hostconfig()['template'])

        #Init a class to handle keyword strings:
        self.keyw = Keyword()

        ##Init Rucio for later uploads and handling:
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
        print("Task: Upload by call")


        #Decide if you select runs from argsparse or just take everythin:
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
        print(run_beg, run_end)
        ts_beg = self.db.FindTimeStamp('number', int(run_beg) )
        ts_end = self.db.FindTimeStamp('number', int(run_end) )

        #Query an overview collection with the relevant information
        #According to the selected timestamps:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'start':True}, from_config=False)
        query = { '$and': [ {'start': {'$gte':ts_beg}}, {'start': {'$lte':ts_end}} ] }
        #collection = self.db.GetQuery(query, sort=[('name',-1)])
        collection = self.db.GetQuery(query)

        #Once you grabbed the overview collection, you want to
        #reset your mongodb projections to include the data field with
        #the locations:
        self.db.Connect()
        self.db.SetProjection(projection={'number':True, 'name':True, '_id':True, 'data':True, 'detector':True, 'start':True}, from_config=False)

        self.check_rucio(collection)
        exit()
        #Run through the overview collection:
        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]
            print("Run: ", r_number, "/", r_name, "-> host:", helper.get_hostconfig('host'))

            #consistency_check:
            #consistency_check(db_info['data'])

            #show database entries:
            #self.show_database_entries(db_info['_id'], db_info['data'])

            ##check meta data:
            #self.check_metadata(db_info['data'])

            #test rucio locations and rules:
            #self.check_rucio(db_info['data'])

    def consistency_check(self, data_field ):
        #consistency check:
        for i_data in data_field:
            if i_data['host'] == 'dali':

                #1) read folder
                folder_content = []
                for (dirpath, dirnames, filenames) in os.walk(i_data['location']):
                    folder_content.extend(filenames)
                    break
                #2) get hashs from files in folder
                nb_hashes = []
                for i_file in folder_content:
                    if len(i_file.split("-")) == 3:
                        hash_ = i_file.split("-")[1]
                        if hash_ not in nb_hashes:
                            nb_hashes.append(hash_)
                #print(nb_hashes)
                #3) get hash from folder
                folder_hash = i_data['location'].split("/")[-1].split("-")[-1]
                #Alarm: if more than one hash is found in the files of a folder
                if len(nb_hashes) > 1:
                    print("Many hashes found in the same directory")
                    print("Location:", i_data['location'])

                #Alarm: if hash folder != hash files
                if nb_hashes[0] != folder_hash:
                    print("Location:", i_data['location'])
                    print("Hash folder:", folder_hash)
                    print("Hash from files:", nb_hashes[0])
                    print("Where:", i_data['host'])
                    print("Do you want to remove entry from DB:")
                    test = input("<y/n> : ")
                    if test == "y":
                        self.db.RemoveDatafield(db_info['_id'], i_data)
                        print("remove also location!!!")

    def show_database_entries(self, _id, data_field ):

        for i_data in data_field:
            print(" <>", i_data.keys() )
            print(" <>", i_data['host'])
            print(" <>", i_data['location'])
            print(" <>", i_data['type'])
            if 'status' in i_data:
                print(" <>", "Status:", i_data['status'] )
            if 'rse' in i_data:
                print(" <>", "RSE:", i_data['rse'])
            if 'destination' in i_data:
                print(" <>", "Destination", i_data['destination'])

            print("________________________________")

            if i_data['host']=="rucio-catalogue":
                if i_data['type'] == 'raw':
                    continue

                print("You are going to reset a Rucio entry for:")
                print(i_data['type'], i_data['host'], i_data['location'])
                test = input("<y/n> : ")
                if test == "y":
                    self.db.SetDataField(_id, type=i_data['type'], host=i_data['host'], key='rse', value=[])
                    self.db.SetDataField(_id, type=i_data['type'], host=i_data['host'], key='status', value="RSEreupload")
                    self.db.SetDataField(_id, type=i_data['type'], host=i_data['host'], key='location', value="n/a")
                    self.db.SetDataField(_id, type=i_data['type'], host=i_data['host'], key='destination', value=[])
                    if 'meta' in i_data:
                        self.db.SetDataField(_id, type=i_data['type'], host=i_data['host'], key='meta', value=None)
                print("---")
                print("Do you want to remove the entry compeletly:")
                test = input("<y/n>: ")
                if test == "y":
                    self.db.RemoveDatafield(_id, i_data)
        print("-----------------------------------")

    def check_metadata(self, data_field ):

        print("Check metadata for STRAX data")
        for i_data in data_field:
            if i_data['host'] != 'rucio-catalogue':
                continue
            if i_data['type'] == 'raw':
                continue
            print("<<<<-----Metadata test:")
            print(" <> ", i_data['type'], " / ", i_data['location'])
            if 'meta' in i_data:
                print("    Meta data exists [X]")
            else:
                print("    Meta data exists [ ]")

    def check_rucio(self, collection):

        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]
            print("Run: ", r_number, "/", r_name, "-> host:", helper.get_hostconfig('host'))


            for i_data in db_info['data']:

                if i_data['host']=="rucio-catalogue":
                    if i_data['type'] == 'raw':
                        continue
                if i_data['host'] == "rucio-catalogue":
                    continue

                #pre infos:
                origin_type     = i_data['type'] #This is the plugin name which is gonna be handled (uploaded)
                origin_location = i_data['location'] #This holds the physical location of the plugin folder
                origin_host     = i_data['host']
                #origin_status   = i_data['status']


                #Extract the template information according the pre-defined physical file structure:
                template_info = self.exp_temp.GetTemplateEval(plugin=origin_type,
                                                                host=origin_host,
                                                                string=origin_location)

                #1) Get experimental dependend rucio structure which needs to be fulfilled from the config file:
                rucio_exp_config_path = helper.get_hostconfig('rucio_template')

                rc_reader = ConfigRucioDataFormat()
                rc_reader.Config(rucio_exp_config_path)
                rucio_template = rc_reader.GetStructure()[origin_type]  #Depending on the plugin we choose the right template from the config
                rucio_template_sorted = [key for key in sorted(rucio_template.keys())]

                #Fill the key words:
                self.keyw.ResetTemplate()
                self.keyw.SetTemplate(template_info)
                self.keyw.SetTemplate(db_info)
                self.keyw.SetTemplate({'science_run': helper.get_science_run(db_info['start'])})

                for key, val, in rucio_template.items():
                    val = self.keyw.CompleteKeywords(val)


                if origin_host != helper.get_hostconfig('host'):
                    continue

                test_scope = val['did'].split(":")[0]
                test_dname = val['did'].split(":")[1]
                print(test_scope, " _ ", test_dname)
                ls_rule = self.rc.rc_api.ListDidRules(test_scope, test_dname)
                nb_rules = len(list(ls_rule))

                if nb_rules == 0:
                    print("Warning", val['did'])










            #print("------test for: ", i_data['location'])
            #ls_rule = self.rc.rc_api.ListDidRules(i_data['location'].split(":")[0], i_data['location'].split(":")[1])

            #for i_rule in ls_rule:
                #print(i_rule)
            #print("---------")
            #print("")





    def __del__(self):
        print( 'Database entries check finishes')
