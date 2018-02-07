# -*- coding: utf-8 -*-
from __future__ import with_statement
import logging

import rucio
from rucio.client.client import Client

from admix.tasks import helper

class Uploader(object):
    
    def __init__(self, db_curser=None,
                       task_list=None,
                       type_list=None,
                       detk_list=None):
        
        self.db_curser = db_curser
        self.task_list = task_list
        self.type_list = type_list
        self.detk_list = detk_list
        
        
    def run(self):
        print(self.task_list)
        print(self.type_list)
        print(self.detk_list)
        
        for i_run in self.db_curser:
            
            run_number = i_run['number']
            run_name   = i_run['name']
            run_data   = i_run['data']
            
            print( run_number )
            for i_data in run_data:
                i_data_type     = i_data['type']
                i_data_status   = i_data['status']
                i_data_host     = i_data['host']
                i_data_location = i_data['location']
                print(i_data_host, i_data_location, i_data_type)