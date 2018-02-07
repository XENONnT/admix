# -*- coding: utf-8 -*-
from __future__ import with_statement
import logging

import rucio
from rucio.client.client import Client

from admix.tasks import helper

class RuleUpdater(Tasker):
    
    def __init__(self, db_curser=None,
                       task_list=None,
                       type_list=None,
                       dest_list=None,
                       detk_list=None):

        self.db_curser = db_curser
        self.task_list = task_list
        self.type_list = type_list
        self.dest_list = dest_list
        self.detk_list = detk_list
        #init the rucio API
        self.rucio_client = Client()
        
    def run(self):
        print(self.task_list)
        print(self.type_list)
        print(self.dest_list)
        print(self.detk_list)
        
        for i_run in self.db_curser:
            
            run_number = i_run['number']
            run_name   = i_run['name']
            
            if 'data' not in i_run:
                continue
            
            run_data   = i_run['data']
            
            for i_data in run_data:                
                
                i_data_type     = i_data['type']
                i_data_status   = i_data['status']
                i_data_host     = i_data['host']
                i_data_location = i_data['location']
                
                if i_data_type not in self.type_list:
                    continue
                
                if i_data_host not in self.dest_list:
                    continue
                
                i_data_rse       = i_data['rse']
                
                if ':' not in i_data_location:
                    continue
                i_data_scope     = i_data_location.split(":")[0]
                i_data_dname     = i_data_location.split(":")[1]
                
                if 'rule_info' in i_data:
                    i_data_rule_info = i_data['rule_info']
                else:
                    continue
                
                
                #get all destinations first:
                destinations = []
                ruleDict = {}
                ruleID = self.rucio_client.list_did_rules(scope=i_data_scope, name=i_data_dname)
                for i_ruleID in ruleID:
                    destinations.append( str(i_ruleID['rse_expression']) )
                    ruleDict[ str(i_ruleID['rse_expression']) ] = {}
                
                #Evaluate the rules:
                ruleID = self.rucio_client.list_did_rules(scope=i_data_scope, name=i_data_dname)
                for i_ruleID in ruleID:
                    ruleDict[ str(i_ruleID['rse_expression']) ]['id'] = i_ruleID['id']
                    ruleDict[ str(i_ruleID['rse_expression']) ]['expires'] = i_ruleID['expires_at']
                    
                    rule_info = self.rucio_client.get_replication_rule( i_ruleID['id'] )
                    ruleDict[ str(i_ruleID['rse_expression']) ]['state'] = rule_info['state']
                    
                
                NewRSE = []
                NewRSES= []
                for j_rse, j_value in ruleDict.items():
                    
                    Rexpires__ = None
                    if j_value['expires'] != None:
                        Rexpires__ = j_value['expires'].strftime("%Y-%m-%d_%H:%M:%S")
                    
                    Rstatus__ = None
                    if j_value['state']:
                        Rstatus__ = j_value['state']
                    
                    stat = ""
                    if Rstatus__ == 'OK':
                        if Rexpires__ == None:
                            stat = "{rse}:{validation}".format(rse=j_rse, validation="valid")
                        else:
                            stat = "{rse}:{validation}".format(rse=j_rse, validation=Rexpires__)
                    else:
                        stat = "{rse}:{validation}".format(rse=j_rse, validation=Rstatus__)
                    
                    NewRSE.append(str(j_rse).decode('unicode-escape'))
                    NewRSES.append( stat.decode('unicode-escape') )
                
                if set(i_data_rse) != set(NewRSE) or set(NewRSES) != set(i_data_rule_info):
                    print("-- Update this run:")
                    print(run_name, run_number, str(i_data_location))
                    print("New entry: ", NewRSE, NewRSES)
                    print("Old entry: ", i_data_rse, i_data_rule_info)
                    print("  ")
                    #cnt_bad_rules+=1
                else:
                    print("-- Nothing to do for run:", run_number)
                    #Need collection pointer:
                    #self.collection.update({'_id': self.run_doc['_id'],
                                            #'data': {
                                                #'$elemMatch': i_data}
                                            #},
                                            #{'$set': {
                                                ##'data.$.status': self.rucio.get_rucio_info()['status'],
                                                ##'data.$.location': self.rucio.get_rucio_info()['location'],
                                                ##'data.$.checksum': self.rucio.get_rucio_info()['checksum'],
                                                #'data.$.rse': NewRSE,
                                                #'data.$.rule_info': NewRSES
                                                #}
                                            #})
                
                
                
                
                
                
                
                
                #new_rse = []
                #new_rule_info = []
                #for i_rule in rucio_rules:
                    #rucio_rule_rse   = i_rule['rse_expression']
                    #rucio_rule_id    = i_rule['id']
                    #rucio_rule_state = i_rule['state']
                    #rucio_rule_expires = i_rule['expires_at']
                    #ruico_rule_account = i_rule['account']
                    #print(rucio_rule_rse, rucio_rule_id)
                    
                    #new_rse.append(rucio_rule_rse)
                    #t = "{rse}:{exp}".format(rse=rucio_rule_rse, exp=rucio_rule_expires.
                    #new_rule_info.append( 
                #rule_history = self.rucio_client.list_replication_rule_full_history(scope=i_data_scope, name=i_data_dname)
                #content = self.rucio_client.list_content(scope=i_data_scope, name=i_data_dname)
                #rucio_file_list = self.rucio_client.get_did(scope=i_data_scope, name=i_data_dname)
                #rucio_metadata = self.rucio_client.get_metadata(scope=i_data_scope, name=i_data_dname)
                
                #for i_h in rule_history:
                    #print(i_h)
                #for i_c in content:
                    #print(i_c)
                #for i_f, iff in rucio_file_list.items():
                    #print(i_f, iff)
                #for i_m, imm in rucio_metadata.items():
                    #print(i_m, imm)
                #print(rule_history)

#1) Extract database information
#2) Verify data