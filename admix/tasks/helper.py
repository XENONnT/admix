# -*- coding: utf-8 -*-
from __future__ import with_statement
#Logger:
import logging
import os
import time
import json
import datetime
import numpy as np

#Create global variables:
global_dictionary = {}

def make_global(dict_key, dict_value):
    global global_dictionary
    
    if dict_key not in global_dictionary:
        global_dictionary[dict_key]=dict_value

def get_hostconfig(key=None):
    with open(global_dictionary['admix_config'], 'r') as data_file:
        data = json.load(data_file)
        if key in data:
            return data[key]
        else:
            return data
    #try:
        #with open(global_dictionary['admix_config'], 'r') as data_file:
            #data = json.load(data_file)
            #return data 
    #except:
        #print("aDMIX host configuration is not loaded")
        #exit()
    
def get_hostname():
    return os.environ.get('HOSTNAME')

def run_number_converter(run_number):
    #convert the run number input from terminal by two
    #operators:
    #  - The ',' separates the individual run numbers
    #  - The '-' separates the individual run numbers by
    #            to create a sequence between run numbers
    nb_array = []
    if run_number != None:
        #splits by ',':
        #get all run numbers which are given by
        #commandline before possible sequence operator
        rn = run_number.split(",")
        
        #split individual array entries by '-'
        #get a continous run number sequence each
        
        for i_rn in rn:
            if i_rn.find('-') >= 0:
                j_rn = i_rn.split("-")
                j_rnA = int(j_rn[0])
                j_rnB = int(j_rn[1])
                j_rnArray = np.arange(j_rnA, j_rnB+1)
                nb_array.extend( j_rnArray )
            else:
                nb_array.append(int(i_rn))
    else:
        nb_array = None
    
    return nb_array

def safeformat(str, **kwargs):
    #https://stackoverflow.com/questions/17215400/python-format-string-unused-named-arguments
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'
    replacements = SafeDict(**kwargs)
    return str.format_map(replacements)

def read_folder(path):
    ret_folder = []
    ret_dirpath = []
    ret_files = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        ret_dirpath.extend(dirpath)
        ret_folder.extend(dirnames)
        ret_files.extend(filenames)
        break
    return [ret_dirpath, ret_folder, ret_files]

def run_name_converter(run_name=None):
    #convert a comma separated list of the --name
    #terminal input into a python  list
    
    if run_name != None:
        run_name = run_name.replace(" ", "")
        run_name = run_name.split(",")
        
    return run_name

def check_valid_timestamp( timestamp=None):
    #Check a valid timestamp scheme input
    # <date (YYMMDD)>_<time (hh:mm)>
    ts_valid = False
    if timestamp != None:
        ts = timestamp.split("_")
        if len(ts) == 2 and len(ts[0]) == 6 and len(ts[1]) == 4:
            ts_valid = True
    
    return ts_valid

def string_to_datatime( time_='700101_0000', pattern='%y%m%d_%H%M'):
    return datetime.datetime.strptime(time_, pattern)

def run_timestampe_converter(timestamp = None):
    
    ts_list = []
    if timestamp != None:
        ts = timestamp.split(",")
        
        for i_ts in ts:
            i_ts = i_ts.replace(" ", "")
            
            if i_ts.find("-") >= 0:
                beg_i_ts = string_to_datatime(i_ts.split("-")[0])
                end_i_ts = string_to_datatime(i_ts.split("-")[1])
                if end_i_ts > beg_i_ts:
                    ts_list.append( "{beg}-{end}".format(beg=i_ts.split("-")[0], end=i_ts.split("-")[1]) )
                    
            else:
                print("You need to define a time range such as: 180101_1530-180101_1630")
    else:
        ts_list=None
    
    return ts_list


