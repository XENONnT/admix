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

def run_number_converter_full(run_number=None):
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
        #(split without, makes no trouble)
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

def eval_run_numbers(run_numbers=None, run_number_min=None, run_number_max=None):

    #test if run_numbers follows a certain structure:
    eval_nb_min = None
    eval_nb_max = None
    if run_numbers != None and run_number_min != None and run_number_max != None:

        if run_numbers.find("-") > 0:
            rn_beg, rn_end = run_numbers.split("-")

            if rn_beg.isdigit():
                eval_nb_min = rn_beg
            elif rn_beg.isdigit() == False and rn_beg == 'MIN':
                eval_nb_min = run_number_min
            else:
                #todo raise exception
                print("Check your run number input (Format: 00000-00002 or 00002")
                exit(1)

            if rn_end.isdigit()==True:
                eval_nb_max = rn_end
            elif rn_end.isdigit()==False and rn_end == 'MAX':
                eval_nb_max = run_number_max
            else:
                # todo raise exception
                print("Check your run number input (Format: 00000-00002 or 00002")
                exit(1)

        elif run_numbers.find("-") ==-1 and run_numbers.isdigit() == True:
            eval_nb_min = run_numbers
            eval_nb_max = run_numbers


    elif run_numbers == None and run_number_min != None and run_number_max != None:
        eval_nb_min = run_number_min
        eval_nb_max = run_number_max
    else:
        #todo raise execption
        print("Check your run number input (Format: 00000-00002 or 00002")
        exit(1)

    return [eval_nb_min, eval_nb_max]

def eval_run_timestamps(run_timestamps=None, run_timestamp_min=None, run_timestamp_max=None):

    eval_ts_min = None
    eval_ts_max = None
    if run_timestamps != None and run_timestamp_min != None and run_timestamp_max != None:

        if run_timestamps.find("-") > 0:

            if run_timestamps.split("-")[0] == 'MIN':
                ts_beg = run_timestamp_min
            else:
                try:
                    ts_beg = string_to_datatime(run_timestamps.split("-")[0])
                except ValueError as e:
                    print("ValueError", e)
                    print("Check your run number input (Format: 180101_1530-180101_1630 or 180101_1530")
                    exit(1)

            if run_timestamps.split("-")[1] == 'MAX':
                ts_end = run_timestamp_max
            else:
                try:
                    ts_end = string_to_datatime(run_timestamps.split("-")[1])
                except ValueError as e:
                    print("ValueError:", e)
                    print("Check your run number input (Format: 180101_1530-180101_1630 or 180101_1530")
                    exit(1)

            if ts_beg < ts_end:
                eval_ts_min = ts_beg
                eval_ts_max = ts_end
            else:
                eval_ts_min = ts_end
                eval_ts_max = ts_beg

        elif run_timestamps.find("-") ==-1 and \
                run_timestamps.split("_")[0].isdigit() and len(run_timestamps.split("_")[0])==6 and \
                run_timestamps.split("_")[1].isdigit() and len(run_timestamps.split("_")[1])==4:

            eval_ts_min = string_to_datatime(run_timestamps)
            eval_ts_max = eval_ts_min
        else:
            print("Check your run number input (Format: 180101_1530-180101_1630 or 180101_1530")
            exit(1)

    elif run_timestamps == None and run_timestamp_min != None and run_timestamp_max != None:
        eval_ts_min = run_timestamp_min
        eval_ts_max = run_timestamp_max
    else:
        #todo raise execption
        print("Check your run number input (Format: 00000-00002 or 00002")
        exit(1)

    return [eval_ts_min, eval_ts_max]


def run_timestamp_converter(timestamp = None):

    ts_list = []
    if timestamp != None:
        ts = timestamp.split(",")

        for i_ts in ts:
            i_ts = i_ts.replace(" ", "")

            if i_ts.find("-") >= 0:
                try:
                    beg_i_ts = string_to_datatime(i_ts.split("-")[0])
                    end_i_ts = string_to_datatime(i_ts.split("-")[1])
                except ValueError as e:
                    print("ValueError:", e)
                    print("You need to define a time range such as: 180101_1530-180101_1630")
                    exit(1)
                if end_i_ts > beg_i_ts:
                    ts_list.append( "{beg}-{end}".format(beg=i_ts.split("-")[0], end=i_ts.split("-")[1]) )
            else:
                pass

    else:
        ts_list=None

    return ts_list


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



#string_to_datatime
def get_science_run(timestamp=datetime.datetime(1981, 11, 11, 5, 30)):
    #Evaluate science run periods:

    if timestamp == datetime.datetime(1981, 11, 11, 5, 30):
        return "0"

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
