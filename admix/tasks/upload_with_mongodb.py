# -*- coding: utf-8 -*-

import logging
import datetime
import time

#import rucio
#from rucio.client.client import Client

#from admix.runDB import xenon_runDB as XenonRunDatabase

import os
from admix.tasks import helper
from admix.interfaces.database import DataBase, ConnectMongoDB
from admix.interfaces.rucioapi import ConfigRucioDataFormat, RucioAPI, RucioCLI, TransferRucio
from admix.interfaces.templater import Templater

class upload_with_mongodb():
    
    def __init__(self):
        print('Upload with mongoDB starts')

    def init(self):
        self.db = ConnectMongoDB()
        self.db.Connect()
        
        #Since we deal with an experiment, everything is predefine:
        self.exp_temp = Templater()
        self.exp_temp.Config( helper.get_hostconfig()['template'] )
        
        
    def run(self,*args, **kwargs):
        self.init()
        print("-------------------------")
        print("Task: Upload with mongodb")
        
        
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
        
        #Run through the overview collection:
        for i_run in collection:
            #Extract run number and name from overview collection
            r_name = i_run['name']
            r_number = i_run['number']
            #Pull the full run information (according to projection which is pre-defined) by the run name
            db_info = self.db.GetRunByName(r_name)[0]
                        
            ###this part is only important if you want to set a certain destination to plugin:
            #on_dali = False
            #on_rucio = False
            #plugin = 'raw_records'
            #for i_data in db_info['data']:
                #if i_data['host'] == 'dali' and i_data['type'] == plugin and os.path.isdir(i_data['location']):
                    #on_dali = True
                #if i_data['host'] == 'rucio-catalogue' and i_data['type'] == plugin:
                    #on_rucio = True
            
            #if on_dali == True and on_rucio == False:
                #for i_data in db_info['data']:
                    #print("  -> ", i_data['type'], i_data['host'], i_data['location'], db_info['_id'])
            
                ##prepare with a destination:
                #self.db.WriteDestination(db_info['_id'], type=plugin, host='dali', destination="rucio-catalogue:UC_OSG_USERDISK:None")
                #self.db.WriteDestination(db_info['_id'], type=plugin, host='dali', destination="rucio-catalogue:NIKHEF_USERDISK:86400")
                
            #find if there is something to do:
            ptr = None
            rse = None
            rlt = None
            rucio_rules = []
            
            for i_data in db_info['data']:
                #We can skip any uploads and downloads if there is no destination:
                if 'destination' not in i_data:
                    continue
                
                #print(i_data['type'], i_data['host'])
                #template_info = self.exp_temp.GetTemplateEval(plugin=i_data['type'], host=i_data['host'], string=i_data['location'])
                #print(template_info)
                #continue
                #analyse destinations:
                b_destination = False
                if len(i_data['destination']) == 0:
                    continue
                elif len(i_data['destination']) == 1:
                    ptr = i_data['destination'][0].split(":")[0] #protocol for upload
                    rse = i_data['destination'][0].split(":")[1] #rse's for upload and destination rule
                    rlt = i_data['destination'][0].split(":")[2] #lifetime in rucio
                    b_destination = True
                elif len(i_data['destination']) > 1:
                    ptr = i_data['destination'][0].split(":")[0] #protocol for upload
                    rse = i_data['destination'][0].split(":")[1] #rse's for uploads, the first one in the list defines the upload destination
                    rlt = i_data['destination'][0].split(":")[2] #lifetime in rucio
                    for i_rule in list(i_data['destination'][1:]):
                        rucio_rules.append(i_rule)
                    b_destination = True
                
                #check if you are at the correct host:
                b_host = False
                if i_data['host'] in helper.get_hostname():
                    b_host = True
                
                
                #decide if a destination can be fulfilled:
                if b_host == True and b_destination == True:
                    print("Start and upload from {host} ({host_domain}) to {aim}".format(host=i_data['host'],
                                                                                         host_domain=helper.get_hostname(),
                                                                                         aim=ptr))
                    
                    #pre info (this is about from where you upload it):
                    dest_type     = i_data['type'] #This is the plugin name which is gonna be handled (uploaded)
                    dest_location = i_data['location'] #This holds the physical location of the plugin folder
                    dest_host     = i_data['host']
                    dest_status   = i_data['status']
                    
                    #Generalize the template stuff later:
                    #Extract the template information according the pre-defined physical file structure:
                    template_info = self.exp_temp.GetTemplateEval(plugin=dest_type,
                                                                  host=i_data['host'],
                                                                  string=i_data['location'])
                    
                    #1) Get experimental dependend rucio structure which needs to be fulfilled
                    rucio_exp_config_path = helper.get_hostconfig('rucio_template')
                    
                    rc_reader = ConfigRucioDataFormat()
                    rc_reader.Config(rucio_exp_config_path)
                    
                    print("rucio types:", rc_reader.GetTypes())
                    rucio_form = rc_reader.GetStructure()[dest_type]
                    for key, val, in rucio_form.items():
                        val = self.complete_destination_tags(val, template_info, db_info)
                    
                    for key, val, in rucio_form.items():
                        print(key, val)
                    #The templater is finished hered

                    #collect all files from the directory:
                    r_path, r_folder, r_files = helper.read_folder(dest_location)
                    
                    #Rename:
                    #This section will not be necessary anymore once we have
                    #Strax ready:
                    print("Begin to rename")
                    
                    #create the template:
                    name_old_template = "{old_name}"
                    
                    name_new_template = {}
                    name_new_template['did'] = "{plugin}-{hash}-{old_name}"
                    name_new_template['tag_words'] = ["plugin", "hash"]
                    name_new_template = self.complete_destination_tags(name_new_template, template_info, db_info)
                    name_new_template = name_new_template['did']
                    
                    for i_file in r_files:
                        #prepare file name:
                        name_old = name_old_template.format(old_name=i_file)
                        name_new = name_new_template.format(old_name=name_old)
                        
                        name_old = os.path.join(dest_location, name_old)
                        name_new = os.path.join(dest_location, name_new)
                        
                        #only rename it when there is no underscore in:
                        if i_file.find("-") >= 0 or len(i_file) > 13:
                            continue
                        
                        #print(name_old, name_new)
                        os.rename(name_old, name_new)
                    print("Renaming done")  
                    
                    
                    rc = TransferRucio()
                    rc.SetAccount(helper.get_hostconfig('rucio_account'))
                    rc.rc_cli.SetConfigPath(helper.get_hostconfig("rucio_cli"))
                    rc.rc_cli.SetProxyTicket(helper.get_hostconfig('rucio_x509'))
                    rc.rc_cli.SetHost(helper.get_hostconfig('host'))
                    rc.rc_cli.SetRucioAccount(helper.get_hostconfig('rucio_account'))
                    rc.rc_cli.ConfigHost()
                    
                    #Set the database to "transferring" while uploading:
                    #self.db.SetStatus(db_info['_id'], type=dest_type, host=dest_host, status="transferring")
                    #print(rse, rlt)
                    
                    #self.db.RemoveDatafield(db_info['_id'], "test")
                    #exit()
                    
                    #Check if rule exists in Rucio:
                    rule_exists = rc.CheckRule(upload_structure=rucio_form, rse=rse)
                    #verify the locations with rucio:
                    verify_location = rc.VerifyLocations(upload_structure=rucio_form, upload_path=dest_location, checksum_test=False)
                    #check for a database entry:
                    db_dest_status = self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='status')
                    db_entry = False
                    
                    if db_dest_status == "dict_not_exists":
                        db_entry = False
                    else:
                        db_entry =True
                    
                    #we need to find out:
                    skip_upload = False
                    
                    if db_entry == False:
                        #Create a new dictionary in the data field for the plugin/host combination
                        new_data_dict={}
                        new_data_dict['location'] = "n/a"
                        new_data_dict['status'] = "transferring"
                        new_data_dict['host'] = ptr
                        new_data_dict['type'] = dest_type
                        if ptr == 'rucio-catalogue':
                            new_data_dict['rse'] = []
                            new_data_dict['destination'] = []
                        
                        self.db.AddDatafield(db_info['_id'], new_data_dict)
                        
                    elif db_entry == True and len(self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse'))>0:
                        list_rse_is = self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse')
                        
                        for i_rse_is in list_rse_is:
                            if i_rse_is.split(":")[0]==rse and i_rse_is.split(":")[1]=='OK':
                                skip_upload = True
                                
                    if skip_upload == True:
                        return 0
                    
                    rc.Upload(upload_structure=rucio_form, upload_path=dest_location, rse=rse, rse_lifetime=rlt, rse_rules=rucio_rules)
                    verify_location = rc.VerifyLocations(upload_structure=rucio_form, upload_path=dest_location, checksum_test=False)
                    rucio_rule = rc.GetRule(upload_structure=rucio_form, rse=rse)
                    
                    #update from:
                    rse_to_db = "{rse}:{state}:{lifetime}".format(rse=rse, state=rucio_rule['state'], lifetime=rucio_rule['eol_at'])
                    self.db.SetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse', value=rse_to_db)
                    if verify_location == True:
                        self.db.SetDataField(db_info['_id'], type=dest_type, host=ptr, key='status', value='transferred')
                    else:
                        self.db.SetDataField(db_info['_id'], type=dest_type, host=ptr, key='status', value='RSEreupload')
                    
                    if rucio_rule['state'] == 'OK' and verify_location == True:
                        print("Upload successful and initial rule exsists")
                    elif rucio_rule['state'] == 'OK' and verify_location == False:
                        print("Rucio transfer rules are ok and data are distributed")
                        print("Nevertheless, there is something wrong with the uploaded data")
                        print(" -> Checksum error")
                        print(" -> Maybe not all files are uploaded")
                    else:
                        print("Verfication of files is sucessful", verify_location)
                        print("Rucio transfer rules are wrong", rse_to_db)
                    
                        
                    print(" ----", dest_type, ptr, ":")
                    print("->", self.db.GetDataField(db_info['_id'], type=dest_type, host=dest_host, key='destination'))
                    print("->", self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse'))
                    print("->", self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='destination'))
                    print("->", self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='status'))
                                
                    
                    print(rucio_rule)
                    
                    #fullfill the remaining transfers and delete destnation:
                    rc.AddRules(upload_structure=rucio_form, rse_rules=rucio_rules)
                    new_db_rse_list = self.db.SetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse')
                    for i_rule in rucio_rules:
                        rse_rule = i_rule.split(":")[1]
                        rucio_rule = rc.GetRule(upload_structure=rucio_form, rse=rse_rule)
                        print(rucio_rule)
                        r_status = rucio_rule['state']
                        r_rse    = rucio_rule['rse_expression']
                        r_expires_at = rucio_rule['expires_at']
                        r_eol_at = rucio_rule['eol_at']
                        
                        new_db_rse = "{rse}:{state}:{eol}".format(rse=r_rse,
                                                                  state=r_status,
                                                                  eol=r_expires_at)
                        new_db_rse_list.append(new_db_rse)                                          
                        
                        print(rse_rule, r_status, r_rse, r_eol_at, r_expires_at)
                    #update the upload rucio rse:
                    self.db.SetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse', value=new_db_rse_list)
                    
                    #remove the destination:
                    self.db.SetDataField(db_info['_id'], type=dest_type, host=dest_host, key='destination', value=[])
                    
                    #for i_rule in rucio_rules:
                        
                        #print("<", i_rule)
                    
                    
                    #db_dest_rse = self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='rse')
                    #db_dest_location = self.db.GetDataField(db_info['_id'], type=dest_type, host=ptr, key='host')
                    
                    #print(ptr, db_dest_status, db_dest_rse, db_dest_location)
                    #if rule_exists == True and verify_location == True:
                        #print("nothing needs to be done")
                    #elif (rule_exists == True and verify_location == False) or (rule_exists == False and verify_location == False):
                        ##run the uploader for re-upload or first upload:
                        #pass
                        #rc.Upload(upload_structure=rucio_form, upload_path=dest_location, rse=rse, rse_lifetime=rlt, rse_rules=rucio_rules)
                        ##check rule:
                        #rucio_rule = rc.GetRule(upload_structure=rucio_form, rse=rse)
                        #if rucio_rule['state'] == 'OK':
                            ##update the database:
                            
                    
                    #print(rucio_rule)
                    #if exists == True and rule != rule_rule:
                        #apply to change the rule (e.g. lifetime)
                    #elif exists == True and rule == rule_rucio:
                        #do nothing
                        #check for further rules if apply
                    #elif exists == False
                        #upload
                        #verify upload
                        #apply further rules
                        
                    #update rules from rucio
                    #update rucio rse expression in db
                    #update destination
                    
                    
                    
                    #rc.VerifyStructure(upload_structure=rucio_form)
                    
                    #rc.Upload(upload_structure=rucio_form, upload_path=dest_location, rse=rse, rse_lifetime=rlt, rse_rules=rucio_rules)
                    
                    success = rc.VerifyLocations(upload_structure=rucio_form, upload_path=dest_location)
                    
                    if success == 1:
                        print("Success")
                        print(i_data['destination'])
                        print(rucio_rules)
                        #self.db.SetDestination()
                        #self.db.SetStatus(db_info['_id'], type=dest_type, host=dest_host, status="transferred")
                        #rc.AddRules(upload_structure=rucio_form, rse_rules=rucio_rules)
                    else:
                        print("fail")
                        #self.db.SetStatus(db_info['_id'], type=dest_type, host=dest_host, status="transferred")
                    
                    #rc.rc_cli.Whoami()
                    
                    #1.1) Set DB for transferring
                    
                    #2) Upload to Rucio
                    #2a) Create/Attach containers and datasets
                    #2b) Upload files
                    #2c) Apply further transfer rules
                    
                    #3) Update the location from Rucio a last time
                    #3a) Update runDB
                    
        
        print("Task: Upload with mongodb [FINISHED]")
        print("-------------------------")
        print("")
        
    def __del__(self):
        print( 'Upload with mongoDB stop')
    
    
    def complete_destination_tags(self, val, template, db_info):
        
        did = val['did']
        #determine the tags which need input information
        for i_tag in val['tag_words']:
            #try to find it in the template:
            if i_tag in template:
                did= did.replace("{"+i_tag+"}", template[i_tag])
            elif self.FromDB(i_tag, db_info) != None:
                did= did.replace("{"+i_tag+"}", self.FromDB(i_tag, db_info))
                
        val['did'] = did
        return val
        
    def FromDB(self, key, db_info):
        if key=="detector":
            return db_info['detector']
        elif key=="science_run":
            sr = self.get_science_run( db_info['start'] )
            if sr != -1:
                return sr
            else:
                return "{"+science_run+"}"
        else:
            return None
        
    def get_science_run(self, timestamp ):
        #Evaluate science run periods:
        
        #1) Change from sc0 to sc1:
        dt0to1 = datetime.datetime(2017, 2, 2, 17, 40)
        
        #Evaluate the according science run number:
        if timestamp <= dt0to1:
            science_run = "000"
        elif timestamp >= dt0to1:
            science_run = "001"
        else:
            science_run = -1
        return science_run     