# -*- coding: utf-8 -*-
from __future__ import with_statement
import logging

from admix.tasks import helper
from admix.runDB import xenon_runDB

import rucio
from rucio.client.client import Client

class RuleUpdater(object):
    
    def __init__(self, db_collection=None,
                       db_curser=None,
                       task_list=None,
                       type_list=None,
                       dest_list=None,
                       detk_list=None):
        self.db_collection = db_collection
        self.db_curser = db_curser
        self.task_list = task_list
        self.type_list = type_list
        self.dest_list = dest_list
        self.detk_list = detk_list
        #init the rucio API
        try:
            self.rucio_client = Client()
        except:
            logging.error("There was an issues with connecting to the Rucio client")
            exit()
        
    def run(self):
        
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
                
                #skip the rucio data locations which are
                #not (yet) available
                if 'n/a' == i_data_location:
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
                    logging.info("Rucio has the latest location information for {r}/{n}".format(r=run_number, n=run_name))
                    logging.info("Old Xenon1T database entry:")
                    for i_old in i_data_rule_info:
                        logging.info("  <> {info}".format(info=i_old))
                    logging.info("New Xenon1T database entry:")
                    for i_new in NewRSES:
                        logging.info("  <> {info}".format(info=i_new))
                    logging.info("-> UPDATE:")
                    
                    if helper.global_dictionary['no_db_update'] == True:
                        self.db_collection.update({'_id': i_run['_id'],
                                                'data':{
                                                            '$elemMatch': i_data
                                                            }
                                                },
                                                {'$set':{
                                                        'data.$.rse': NewRSE,
                                                        'data.$.rule_info': NewRSES
                                                    }
                                                    })
                        logging.info("    -> Success")
                    else:
                        logging.info("Xenon database update is disabled")
                else:
                    pass
                    #print("-- Nothing to do for run:", run_number)

